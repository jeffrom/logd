package events

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

func startHandlerConfig(t testing.TB, conf *config.Config) *Handlers {
	t.Helper()
	h := NewHandlers(conf)
	t.Logf("starting event queue with config: %+v", conf)
	if err := h.GoStart(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error starting queue: %+v", err)
	}
	return h
}

func startHandler(t *testing.T) *Handlers {
	t.Helper()
	return startHandlerConfig(t, testhelper.DefaultConfig(testing.Verbose()))
}

func stopHandler(t testing.TB, h *Handlers) {
	t.Helper()
	if err := h.Stop(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error stopping queue: %+v", err)
	}
}

func TestHandlerStartStop(t *testing.T) {
	h := startHandler(t)
	stopHandler(t, h)
}

type testConfigs map[string]*config.Config

func (tc testConfigs) forEach(cb func(*config.Config) *config.Config) testConfigs {
	for name, conf := range tc {
		tc[name] = cb(conf)
	}
	return tc
}

func buildTestConfigs() testConfigs {
	confs := make(testConfigs)

	c := testhelper.DefaultConfig(testing.Verbose())
	confs["default"] = c

	if testing.Short() {
		return confs
	}

	c = testhelper.DefaultConfig(testing.Verbose())
	c.FlushBatches = 1
	confs["batch sync"] = c

	c = testhelper.DefaultConfig(testing.Verbose())
	c.FlushInterval = 1 * time.Millisecond
	confs["interval sync"] = c

	c = testhelper.DefaultConfig(testing.Verbose())
	c.FlushBatches = 100
	confs["batch sync 100"] = c

	c = testhelper.DefaultConfig(testing.Verbose())
	c.FlushInterval = 500 * time.Millisecond
	confs["interval sync 500ms"] = c

	return confs
}

func TestHandlerFileLogger(t *testing.T) {
	confs := buildTestConfigs().forEach(func(conf *config.Config) *config.Config {
		conf.MaxBatchSize /= 20
		conf.PartitionSize /= 20
		return conf
	})

	for name, conf := range confs {
		t.Run(name, func(t *testing.T) {
			// goal is to test every rotation case
			runs := conf.MaxBatchSize
			for i := 0; i < runs; i++ {
				conf.PartitionSize -= i % 10
				if i%10 == 9 {
					conf.PartitionSize /= 2
				}

				if conf.PartitionSize < conf.MaxBatchSize {
					break
				}

				testHandlerFileLogger(t, conf)

				if conf.PartitionSize <= 1 {
					break
				}
			}
		})
	}
}

func testHandlerFileLogger(t *testing.T, conf *config.Config) {
	t.Logf("testing with config: %+v", conf)
	h := NewHandlers(conf)
	doStartHandler(t, h)
	defer doShutdownHandler(t, h)

	fixture := testhelper.LoadFixture("batch.small")
	writesPerPartition := conf.PartitionSize / len(fixture)
	n, interval := partitionIterations(conf, len(fixture))
	var offs []uint64

	for i := 0; i < n; i += interval {
		// trim offsets from rotated partitions
		if i > 0 && i%writesPerPartition == 0 {
			offs = offs[writesPerPartition:]
		}

		cr := pushBatch(t, h, fixture)

		checkBatch(t, h, fixture, cr.Offset(), 1)
		checkReadMultipleBatches(t, h, fixture, offs)

		testhelper.CheckError(h.Stop())
		testhelper.CheckError(h.GoStart())

		cr = pushBatch(t, h, fixture)

		checkBatch(t, h, fixture, cr.Offset(), 1)
		checkReadMultipleBatches(t, h, fixture, offs)

		offs = append(offs, cr.Offset())
	}
}

func addReadRespEnvelope(off uint64, batches int, b []byte) []byte {
	return append([]byte(fmt.Sprintf("OK %d %d\r\n", off, batches)), b...)
}

func checkBatch(t *testing.T, h *Handlers, fixture []byte, off uint64, batches int) {
	t.Helper()
	respb := pushRead(t, h, off, 3)
	expect := addReadRespEnvelope(off, batches, fixture)
	if !bytes.Equal(respb, expect) {
		log.Panicf("expected (%d):\n\t%q\nbut got\n\t%q", off, expect, respb)
		// t.Fatalf("expected (%d):\n\t%q\nbut got\n\t%q", off, fixture, respb)
	}
}

func checkReadMultipleBatches(t *testing.T, h *Handlers, fixture []byte, offs []uint64) {
	t.Helper()
	if len(offs) <= 1 {
		return
	}
	for i, off := range offs {
		left := len(offs) - i
		if left <= 1 {
			break
		}

		remainingMessages := (left * 3)
		for j := 0; j < 3; j++ {
			respb := pushRead(t, h, off, remainingMessages-j)
			envelope := []byte(fmt.Sprintf("OK %d %d\r\n", off, (remainingMessages-j)/3))
			if len(respb)-len(envelope) != len(fixture)*left {
				t.Logf("failed attempt at READ('default', %d, %d), expected %d remaining batches. Log location: %s", off, remainingMessages, left, h.conf.WorkDir)
				log.Panicf("expected (%d):\n\t(%dx)%q\nbut got\n\t%q", off, left, fixture, respb)
			}
		}
	}
}

func TestPartitionRemoval(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	h := NewHandlers(conf)
	doStartHandler(t, h)
	defer doShutdownHandler(t, h)

	topic, err := h.topics.get("default")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < conf.MaxPartitions*3; i++ {
		fillPartition(t, h)
		parts, err := topic.parts.logp.List()
		if err != nil {
			t.Fatalf("unexpected failure listing partitions: %+v", err)
		}

		if len(parts) > conf.MaxPartitions {
			t.Fatalf("expected %d or less partitions but there were %d", conf.MaxPartitions, len(parts))
		}
		if i >= conf.MaxPartitions && len(parts) < conf.MaxPartitions {
			t.Fatalf("expected %d partitions but there were %d", conf.MaxPartitions, len(parts))
		}
	}
}

func TestReadNotFound(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	h := NewHandlers(conf)
	doStartHandler(t, h)
	defer doShutdownHandler(t, h)

	for i := 0; i < conf.MaxPartitions*3; i++ {
		offs := fillPartition(t, h)
		for _, off := range offs {
			if off > 0 {
				checkNotFound(t, conf, pushRead(t, h, off-1, 3))
			}
			if off > 10 {
				checkNotFound(t, conf, pushRead(t, h, off-9, 3))
			}
			checkNotFound(t, conf, pushRead(t, h, off+1, 3))
			checkNotFound(t, conf, pushRead(t, h, off+10, 3))
			checkNotFound(t, conf, pushRead(t, h, off+100, 3))
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	h := NewHandlers(conf)
	doStartHandler(t, h)
	defer doShutdownHandler(t, h)

	req := protocol.NewRequestConfig(conf)
	resp, err := h.PushRequest(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	r, err := resp.ScanReader()
	if err != nil {
		t.Fatal(err)
	}
	cr := protocol.NewClientResponseConfig(conf)
	if _, rerr := cr.ReadFrom(bufio.NewReader(r)); rerr != nil {
		t.Fatalf("unexpected error reading response: %+v", rerr)
	}
	if err := cr.Error(); err != protocol.ErrInvalid {
		t.Fatalf("expected error %s but got %s", protocol.ErrInvalid, err)
	}
}

func checkNotFound(t testing.TB, conf *config.Config, b []byte) {
	t.Helper()
	if !bytes.HasPrefix(b, []byte("ERR")) {
		log.Panicf("response was not an error: %q", b)
	}
}

func newRequest(t testing.TB, conf *config.Config, p []byte) *protocol.Request {
	t.Helper()
	req := protocol.NewRequestConfig(conf)

	_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(p)))
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return req
}

func requestSet(t testing.TB, conf *config.Config, req *protocol.Request, buf []byte, b *bytes.Buffer, br *bufio.Reader) {
	t.Helper()
	b.Reset()
	// fmt.Printf("%q\n", buf)
	if _, err := b.Write(buf); err != nil {
		t.Fatalf("failed to write request into buffer: %+v", err)
	}
	// *bufio.Reader resets can cause the underlying buffer to be mutated
	br.Reset(nil)
	br.Reset(b)

	req.Reset()
	if _, err := req.ReadFrom(br); err != nil {
		t.Fatalf("failed to read request from buffer: %+v", err)
	}
}

func checkBatchResp(t testing.TB, conf *config.Config, resp *protocol.Response) *protocol.ClientResponse {
	t.Helper()
	if resp.NumReaders() != 1 {
		log.Panicf("expected 1 reader but got %d", resp.NumReaders())
	}

	r, err := resp.ScanReader()
	if err != nil {
		log.Panicf("unexpected error scanning response reader: %+v", err)
	}
	defer r.Close()

	cr := protocol.NewClientResponseConfig(conf)
	if _, rerr := cr.ReadFrom(bufio.NewReader(r)); rerr != nil {
		log.Panicf("unexpected error reading batch: %+v", rerr)
	}
	return cr
}

func checkReadResp(t testing.TB, conf *config.Config, resp *protocol.Response) []byte {
	t.Helper()
	b := &bytes.Buffer{}
	for {
		r, err := resp.ScanReader()
		if err != nil {
			t.Fatalf("unexpected error scanning response reader: %+v", err)
		}
		if r == nil {
			break
		}
		defer r.Close()

		if _, err := b.ReadFrom(r); err != nil {
			t.Fatalf("unexpected error reading batch: %+v", err)
		}
	}

	return b.Bytes()
}

func doStartHandler(b testing.TB, h *Handlers) {
	b.Helper()
	dir, _ := filepath.Split(h.conf.WorkDir)
	log.Printf("starting log dir: %s", dir)
	if err := h.GoStart(); err != nil {
		b.Fatalf("unexpected error starting event queue: %+v", err)
	}
}

func doShutdownHandler(t testing.TB, h *Handlers) {
	t.Helper()
	if t.Failed() {
		t.Logf("failed: not removing files in %s", h.conf.WorkDir)
		return
	}
	if err := h.Stop(); err != nil {
		t.Fatalf("unexpected error shutting down: %+v", err)
	}
}

// writes a partition worth of batch requests into the h
func fillPartition(t testing.TB, h *Handlers) []uint64 {
	t.Helper()
	var offs []uint64
	fixture := testhelper.LoadFixture("batch.small")
	n := 0
	for n+len(fixture) < h.conf.PartitionSize {
		cr := pushBatch(t, h, fixture)
		offs = append(offs, cr.Offset())
		n += len(fixture)
	}
	return offs
}

func pushBatch(t testing.TB, h *Handlers, fixture []byte) *protocol.ClientResponse {
	t.Helper()
	ctx := context.Background()
	req := newRequest(t, h.conf, fixture)
	resp, err := h.PushRequest(ctx, req)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return checkBatchResp(t, h.conf, resp)
}

func pushReadTopic(t testing.TB, h *Handlers, topic string, off uint64, limit int) []byte {
	t.Helper()
	ctx := context.Background()
	fixture := []byte(fmt.Sprintf("READ %s %d %d\r\n", topic, off, limit))
	req := newRequest(t, h.conf, fixture)
	resp, err := h.PushRequest(ctx, req)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return checkReadResp(t, h.conf, resp)
}

func pushRead(t testing.TB, h *Handlers, off uint64, limit int) []byte {
	t.Helper()
	return pushReadTopic(t, h, "default", off, limit)
}

func partitionIterations(conf *config.Config, fixtureLen int) (int, int) {
	n := (conf.PartitionSize / fixtureLen) * (conf.MaxPartitions + 5)
	interval := 1
	if testing.Short() {
		n /= 10
		if n == 0 {
			n = 1
		}
		interval = 1
	}
	return n, interval
}
