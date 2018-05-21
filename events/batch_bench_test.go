package events

import (
	"bufio"
	"bytes"
	"context"
	"testing"

	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkBatchV2(b *testing.B) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	mw := logger.NewDiscardWriter(conf)
	q.logw = mw
	if err := q.Start(); err != nil {
		b.Fatalf("unexpected startup error: %+v", err)
	}

	ctx := context.Background()
	fixture := testhelper.LoadFixture("batch.small")
	req := protocol.NewRequest(conf)

	_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture)))
	if err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.PushRequest(ctx, req)
		if err != nil {
			b.Fatalf("%+v", err)
		}
	}
}
