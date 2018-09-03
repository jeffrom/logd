package events

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkReadHead(b *testing.B) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	q := NewHandlers(conf)
	doStartHandler(b, q)
	defer doShutdownHandler(b, q)
	offs := writeBatches(b, conf, q)
	benchmarkRead(b, conf, q, offs[len(offs)-1:])
}

func BenchmarkReadTail(b *testing.B) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	q := NewHandlers(conf)
	doStartHandler(b, q)
	defer doShutdownHandler(b, q)
	offs := writeBatches(b, conf, q)
	benchmarkRead(b, conf, q, offs[:1])
}

func BenchmarkReadAll(b *testing.B) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	q := NewHandlers(conf)
	doStartHandler(b, q)
	defer doShutdownHandler(b, q)
	offs := writeBatches(b, conf, q)
	benchmarkRead(b, conf, q, offs)
}

func benchmarkRead(b *testing.B, conf *config.Config, q *Handlers, offs []uint64) {
	var bufs [][]byte
	for _, off := range offs {
		buf := []byte(fmt.Sprintf("READ default %d %d\r\n", off, 3))
		bufs = append(bufs, buf)
	}
	// b.Logf("%d read commands prepared", len(bufs))

	n := 0
	buf := bufs[n]
	bbuf := bytes.NewBuffer(buf)
	// if it's too small, resets mutate the underlying buffer
	br := bufio.NewReaderSize(bbuf, len(bufs[len(bufs)-1])*2)
	req := protocol.NewRequestConfig(conf)
	requestSet(b, conf, req, buf, bbuf, br)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Response.Reset()
		_, err := q.PushRequest(ctx, req)
		if err != nil {
			b.Fatalf("unexpected error doing read: %+v", err)
		}

		if len(offs) > 1 {
			n++
			if n >= len(offs) {
				n = 0
			}

			requestSet(b, conf, req, buf, bbuf, br)
		}
	}
}

func writeBatches(b testing.TB, conf *config.Config, q *Handlers) []uint64 {
	ctx := context.Background()
	fixture := testhelper.LoadFixture("batch.small")
	req := protocol.NewRequestConfig(conf)

	_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture)))
	if err != nil {
		b.Fatalf("unexpected error building request: %+v", err)
	}

	var offs []uint64
	n := conf.MaxPartitions * len(fixture)
	for i := 0; i < n; i++ {
		req.Response.Reset()
		resp, err := q.PushRequest(ctx, req)
		if err != nil {
			b.Fatalf("unexpected error writing batches: %+v", err)
		}
		cr := checkBatchResp(b, conf, resp)
		offs = append(offs, cr.Offset())
	}
	return offs
}
