package events

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestEventQBatchV2(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	mw := logger.NewMockWriter(conf)
	q.logw = mw
	if err := q.Start(); err != nil {
		t.Fatalf("%+v", err)
	}

	ctx := context.Background()
	fixture := testhelper.LoadFixture("batch.small")
	req := newRequest(t, conf, fixture)

	resp, err := q.PushRequest(ctx, req)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	checkBatchResp(t, conf, resp)

	parts := mw.Partitions()
	if len(parts) != 1 {
		t.Fatalf("Expected 1 log partition but got %d", len(parts))
	}

	actual := parts[0].Bytes()
	testhelper.CheckGoldenFile("batch.small", actual, testhelper.Golden)

	n, interval := partitionIterations(conf, len(fixture))

	q.logw = logger.NewDiscardWriter(conf)

	for i := 0; i < n; i += interval {
		resp, err = q.PushRequest(ctx, req)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		cr := checkBatchResp(t, conf, resp)
		expectedOffset := calcOffset(len(fixture), i, interval)
		if cr.Offset() != expectedOffset {
			t.Fatalf("expected next message offset to be %d, but was %d", expectedOffset, cr.Offset())
		}
	}
}

func TestQFileLoggerV2(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	t.Log(conf)
	q := NewEventQ(conf)
	if err := q.Start(); err != nil {
		t.Fatalf("%+v", err)
	}

	fixture := testhelper.LoadFixture("batch.small")
	n, interval := partitionIterations(conf, len(fixture))
	ctx := context.Background()
	for i := 0; i < n; i += interval {
		req := newRequest(t, conf, fixture)
		resp, err := q.PushRequest(ctx, req)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		cr := checkBatchResp(t, conf, resp)
		b := []byte(fmt.Sprintf("READV2 %d %d\r\n", cr.Offset(), 3))
		req = newRequest(t, conf, b)
		resp, err = q.PushRequest(ctx, req)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		respb := checkReadResp(t, conf, resp)
		if !bytes.Equal(respb, fixture) {
			t.Fatalf("expected:\n\t%q\nbut got\n\t%q", fixture, respb)
		}

		testhelper.CheckError(q.Stop())
		testhelper.CheckError(q.Start())

		req = newRequest(t, conf, fixture)
		resp, err = q.PushRequest(ctx, req)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		cr = checkBatchResp(t, conf, resp)
		b = []byte(fmt.Sprintf("READV2 %d %d\r\n", cr.Offset(), 3))
		req = newRequest(t, conf, b)
		resp, err = q.PushRequest(ctx, req)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		respb = checkReadResp(t, conf, resp)
		if !bytes.Equal(respb, fixture) {
			t.Fatalf("expected:\n\t%q\nbut got\n\t%q", fixture, respb)
		}
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

func checkBatchResp(t testing.TB, conf *config.Config, resp *protocol.ResponseV2) *protocol.ClientResponse {
	if resp.NumReaders() != 1 {
		t.Fatalf("expected 1 reader but got %d", resp.NumReaders())
	}

	r, err := resp.ScanReader()
	if err != nil {
		t.Fatalf("unexpected error scanning response reader: %+v", err)
	}

	cr := protocol.NewClientResponse(conf)
	if _, rerr := cr.ReadFrom(bufio.NewReader(r)); rerr != nil {
		t.Fatalf("unexpected error reading batch: %+v", rerr)
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
