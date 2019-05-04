package events

import (
	"bufio"
	"bytes"
	"context"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

// func BenchmarkBatchDiscard(b *testing.B) {
// 	conf := testhelper.DefaultTestConfig(testing.Verbose())
// 	mw := logger.NewDiscardWriter(conf)
// 	benchmarkBatch(b, conf, mw)
// }

func BenchmarkBatchFile(b *testing.B) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	benchmarkBatch(b, conf, nil)
}

func benchmarkBatch(b *testing.B, conf *config.Config, logw logger.LogWriter) {
	h := NewHandlers(conf)
	if err := h.GoStart(); err != nil {
		b.Fatalf("unexpected startup error: %+v", err)
	}
	defer shutdownHandlers(b, h)

	ctx := context.Background()
	fixture := testhelper.LoadFixture("batch.small")
	req := protocol.NewRequestConfig(conf)

	_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture)))
	if err != nil {
		b.Fatalf("unexpected error building request: %+v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Response.Reset()
		_, err := h.PushRequest(ctx, req)
		if err != nil {
			b.Fatalf("unexpected error writing batches: %+v", err)
		}

		// r, err := resp.ScanReader()
		// if err != nil {
		// 	b.Fatal(err)
		// }
		// cr := protocol.NewClientResponse(conf)
		// if _, rerr := cr.ReadFrom(bufio.NewReader(r)); rerr != nil {
		// 	b.Fatalf("unexpected error reading response: %+v", rerr)
		// }
	}
}
