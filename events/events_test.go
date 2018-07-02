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

func startQConfig(t testing.TB, conf *config.Config) *EventQ {
	q := NewEventQ(conf)
	t.Logf("starting event queue with config: %+v", conf)
	if err := q.GoStart(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error starting queue: %+v", err)
	}
	return q
}

func startQ(t *testing.T) *EventQ {
	return startQConfig(t, testhelper.TestConfig(testing.Verbose()))
}

func stopQ(t testing.TB, q *EventQ) {
	if err := q.Stop(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error stopping queue: %+v", err)
	}
}

func TestEventQStartStop(t *testing.T) {
	q := startQ(t)
	stopQ(t, q)
}

func TestQFileLogger(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	conf.MaxBatchSize /= 20
	conf.PartitionSize /= 20

	// goal is to test every rotation case
	runs := conf.MaxBatchSize
	for i := 0; i < runs; i++ {
		conf.PartitionSize--
		testQFileLogger(t, conf)
	}
}

func testQFileLogger(t *testing.T, conf *config.Config) {
	t.Logf("testing with config: %s", conf)
	q := NewEventQ(conf)
	doStartQ(t, q)
	defer doShutdownQ(t, q)

	fixture := testhelper.LoadFixture("batch.small")
	writesPerPartition := conf.PartitionSize / len(fixture)
	n, interval := partitionIterations(conf, len(fixture))
	var offs []uint64

	for i := 0; i < n; i += interval {
		// trim offsets from rotated partitions
		if i > 0 && i%writesPerPartition == 0 {
			offs = offs[writesPerPartition:]
		}

		cr := pushBatch(t, q, fixture)

		checkBatch(t, q, fixture, cr.Offset())
		checkReadMultipleBatches(t, q, fixture, offs)

		testhelper.CheckError(q.Stop())
		testhelper.CheckError(q.GoStart())

		cr = pushBatch(t, q, fixture)

		checkBatch(t, q, fixture, cr.Offset())
		checkReadMultipleBatches(t, q, fixture, offs)

		offs = append(offs, cr.Offset())
	}
}

func addReadRespEnvelope(off uint64, b []byte) []byte {
	return append([]byte(fmt.Sprintf("OK %d\r\n", off)), b...)
}

func checkBatch(t *testing.T, q *EventQ, fixture []byte, off uint64) {
	respb := pushRead(t, q, off, 3)
	if !bytes.Equal(respb, addReadRespEnvelope(off, fixture)) {
		t.Fatalf("expected (%d):\n\t%q\nbut got\n\t%q", off, fixture, respb)
	}
}

func checkReadMultipleBatches(t *testing.T, q *EventQ, fixture []byte, offs []uint64) {
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
			respb := pushRead(t, q, off, remainingMessages-j)
			envelope := []byte(fmt.Sprintf("OK %d\r\n", off))
			if len(respb)-len(envelope) != len(fixture)*left {
				t.Logf("failed attempt at READ(%d, %d), expected %d remaining batches. Log location: %s", off, remainingMessages, left, q.conf.LogFile)
				log.Panicf("expected (%d):\n\t(%dx)%q\nbut got\n\t%q", off, left, fixture, respb)
			}
		}
	}
}

func TestPartitionRemoval(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	doStartQ(t, q)
	defer doShutdownQ(t, q)

	for i := 0; i < conf.MaxPartitions*3; i++ {
		fillPartition(t, q)
		parts, err := q.parts.logp.List()
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
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	doStartQ(t, q)
	defer doShutdownQ(t, q)

	for i := 0; i < conf.MaxPartitions*3; i++ {
		offs := fillPartition(t, q)
		for _, off := range offs {
			if off > 0 {
				checkNotFound(t, conf, pushRead(t, q, off-1, 3))
			}
			if off > 10 {
				checkNotFound(t, conf, pushRead(t, q, off-9, 3))
			}
			checkNotFound(t, conf, pushRead(t, q, off+1, 3))
			checkNotFound(t, conf, pushRead(t, q, off+10, 3))
			checkNotFound(t, conf, pushRead(t, q, off+100, 3))
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	doStartQ(t, q)
	defer doShutdownQ(t, q)

	req := protocol.NewRequest(conf)
	resp, err := q.PushRequest(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	r, err := resp.ScanReader()
	if err != nil {
		t.Fatal(err)
	}
	cr := protocol.NewClientResponse(conf)
	if _, rerr := cr.ReadFrom(bufio.NewReader(r)); rerr != nil {
		t.Fatalf("unexpected error reading response: %+v", rerr)
	}
	if err := cr.Error(); err != protocol.ErrInvalid {
		t.Fatalf("expected error %s but got %s", protocol.ErrInvalid, err)
	}
}

func checkNotFound(t testing.TB, conf *config.Config, b []byte) {
	if !bytes.HasPrefix(b, []byte("ERR")) {
		log.Panicf("response was not an error: %q", b)
	}
}

func newRequest(t testing.TB, conf *config.Config, p []byte) *protocol.Request {
	req := protocol.NewRequest(conf)

	_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(p)))
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return req
}

func requestSet(t testing.TB, conf *config.Config, req *protocol.Request, buf []byte, b *bytes.Buffer, br *bufio.Reader) {
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
	if resp.NumReaders() != 1 {
		log.Panicf("expected 1 reader but got %d", resp.NumReaders())
	}

	r, err := resp.ScanReader()
	if err != nil {
		log.Panicf("unexpected error scanning response reader: %+v", err)
	}
	defer r.Close()

	cr := protocol.NewClientResponse(conf)
	if _, rerr := cr.ReadFrom(bufio.NewReader(r)); rerr != nil {
		log.Panicf("unexpected error reading batch: %+v", rerr)
	}
	return cr
}

func checkReadResp(t testing.TB, conf *config.Config, resp *protocol.Response) []byte {
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

func doStartQ(b testing.TB, q *EventQ) {
	dir, _ := filepath.Split(q.conf.LogFile)
	log.Printf("starting log dir: %s", dir)
	if err := q.GoStart(); err != nil {
		b.Fatalf("unexpected error starting event queue: %+v", err)
	}
}

func doShutdownQ(t testing.TB, q *EventQ) {
	if t.Failed() {
		t.Logf("failed: not removing files in %s", q.conf.LogFile)
		return
	}
	if err := q.Stop(); err != nil {
		t.Fatalf("unexpected error shutting down: %+v", err)
	}
}

// writes a partition worth of batch requests into the q
func fillPartition(t testing.TB, q *EventQ) []uint64 {
	var offs []uint64
	fixture := testhelper.LoadFixture("batch.small")
	n := 0
	for n+len(fixture) < q.conf.PartitionSize {
		cr := pushBatch(t, q, fixture)
		offs = append(offs, cr.Offset())
		n += len(fixture)
	}
	return offs
}

func pushBatch(t testing.TB, q *EventQ, fixture []byte) *protocol.ClientResponse {
	ctx := context.Background()
	req := newRequest(t, q.conf, fixture)
	resp, err := q.PushRequest(ctx, req)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return checkBatchResp(t, q.conf, resp)
}

func pushRead(t testing.TB, q *EventQ, off uint64, limit int) []byte {
	ctx := context.Background()
	fixture := []byte(fmt.Sprintf("READ %d %d\r\n", off, limit))
	req := newRequest(t, q.conf, fixture)
	resp, err := q.PushRequest(ctx, req)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return checkReadResp(t, q.conf, resp)
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

func calcOffset(l, i, interval int) uint64 {
	return uint64(l * ((i / interval) + 1))
}
