package events

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestQFileLoggerV2(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	startQV2(t, q)
	defer shutdownQV2(t, q)

	fixture := testhelper.LoadFixture("batch.small")
	n, interval := partitionIterations(conf, len(fixture))
	for i := 0; i < n; i += interval {
		cr := pushBatch(t, q, fixture)
		respb := pushRead(t, q, cr.Offset(), 3)
		if !bytes.Equal(respb, fixture) {
			t.Fatalf("expected (%d):\n\t%q\nbut got\n\t%q", cr.Offset(), fixture, respb)
		}

		testhelper.CheckError(q.Stop())
		testhelper.CheckError(q.Start())

		cr = pushBatch(t, q, fixture)
		respb = pushRead(t, q, cr.Offset(), 3)
		if !bytes.Equal(respb, fixture) {
			t.Fatalf("expected (%d):\n\t%q\nbut got\n\t%q", cr.Offset(), fixture, respb)
		}
	}
}

func TestPartitionRemovalV2(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	startQV2(t, q)
	defer shutdownQV2(t, q)

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

func TestReadNotFoundV2(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	startQV2(t, q)
	defer shutdownQV2(t, q)

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

func checkBatchResp(t testing.TB, conf *config.Config, resp *protocol.ResponseV2) *protocol.ClientResponse {
	if resp.NumReaders() != 1 {
		log.Panicf("expected 1 reader but got %d", resp.NumReaders())
	}

	r, err := resp.ScanReader()
	if err != nil {
		log.Panicf("unexpected error scanning response reader: %+v", err)
	}

	cr := protocol.NewClientResponse(conf)
	if _, rerr := cr.ReadFrom(bufio.NewReader(r)); rerr != nil {
		log.Panicf("unexpected error reading batch: %+v", rerr)
	}
	return cr
}

func checkReadResp(t testing.TB, conf *config.Config, resp *protocol.ResponseV2) []byte {
	if resp.NumReaders() != 1 {
		t.Fatalf("expected 1 reader but got %d", resp.NumReaders())
	}
	r, err := resp.ScanReader()
	if err != nil {
		t.Fatalf("unexpected error scanning response reader: %+v", err)
	}
	b := &bytes.Buffer{}
	if _, err := b.ReadFrom(r); err != nil {
		t.Fatalf("unexpected error reading batch: %+v", err)
	}
	return b.Bytes()
}

func startQV2(b testing.TB, q *EventQ) {
	dir, _ := filepath.Split(q.conf.LogFile)
	log.Printf("starting log dir: %s", dir)
	if err := q.Start(); err != nil {
		b.Fatalf("unexpected error starting event queue: %+v", err)
	}
}

func shutdownQV2(t testing.TB, q *EventQ) {
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
	fixture := []byte(fmt.Sprintf("READV2 %d %d\r\n", off, limit))
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
		n = 2
		interval = 1
	}
	return n, interval
}

func calcOffset(l, i, interval int) uint64 {
	return uint64(l * ((i / interval) + 1))
}
