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

func BenchmarkReadV2(b *testing.B) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	if err := q.Start(); err != nil {
		b.Fatalf("unexpected error starting event queue: %+v", err)
	}
	offsets := writeBatches(b, conf, q)
	offset := offsets[len(offsets)-1]
	buf := []byte(fmt.Sprintf("READV2 %d %d\r\n", offset, 3))
	req := newRequest(b, conf, buf)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.PushRequest(ctx, req)
		if err != nil {
			b.Fatalf("unexpected error doing read: %+v", err)
		}
	}
}

func writeBatches(b testing.TB, conf *config.Config, q *EventQ) []uint64 {
	ctx := context.Background()
	fixture := testhelper.LoadFixture("batch.small")
	req := protocol.NewRequest(conf)

	_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture)))
	if err != nil {
		b.Fatalf("unexpected error building request: %+v", err)
	}

	var offs []uint64
	for i := 0; i < 1000; i++ {
		resp, err := q.PushRequest(ctx, req)
		if err != nil {
			b.Fatalf("unexpected error writing batches: %+v", err)
		}
		cr := checkBatchResp(b, conf, resp)
		offs = append(offs, cr.Offset())
	}
	return offs
}
