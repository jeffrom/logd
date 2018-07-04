package events

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/server"
	"github.com/jeffrom/logd/testhelper"
)

func TestMockServer(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q, _, shutdown := newMockServerQ(t, conf)

	doStartQ(t, q)
	shutdown()
}

func TestMockServerClose(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q, s, shutdown := newMockServerQ(t, conf)
	doStartQ(t, q)
	defer shutdown()
	cconf := client.DefaultTestConfig(testing.Verbose())
	_, clientShutdown := newMockServerClient(t, cconf, s)
	clientShutdown()
}

func TestMockServerBatchAndRead(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q, s, shutdown := newMockServerQ(t, conf)
	doStartQ(t, q)
	defer shutdown()
	cconf := client.DefaultTestConfig(testing.Verbose())
	client, clientShutdown := newMockServerClient(t, cconf, s)
	defer clientShutdown()

	fixture := testhelper.LoadFixture("batch.small")
	batch := protocol.NewBatch(conf)
	if _, rerr := batch.ReadFrom(bufio.NewReader(bytes.NewReader(fixture))); rerr != nil {
		t.Fatal(rerr)
	}

	off, err := client.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	if off != 0 {
		t.Fatalf("expected offset 0 but got %d", off)
	}

	_, scanner, err := client.ReadOffset(off, 3)
	if err != nil {
		t.Fatal(err)
	}

	ok := scanner.Scan()
	if !ok {
		t.Fatal("expected to scan one batch")
	}
	fmt.Printf("read: %q\n", scanner.Batch().Bytes())
	if serr := scanner.Error(); serr != nil && serr != io.EOF {
		t.Fatal(serr)
	}

	// 	off, err = client.Batch(batch)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	if off != uint64(len(fixture)) {
	// 		t.Fatalf("expected offset %d but got %d", len(fixture), off)
	// 	}
}

func newMockServerQ(t testing.TB, conf *config.Config) (*EventQ, *server.MockSocket, func()) {
	q := NewEventQ(conf)
	s := server.NewMockSocket(conf)
	q.Register(s)
	return q, s, func() {
		if err := q.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

func newMockServerClient(t testing.TB, conf *client.Config, s *server.MockSocket) (*client.Client, func()) {
	c, err := s.Dial()
	if err != nil {
		t.Fatal(err)
	}
	client := client.New(client.DefaultTestConfig(testing.Verbose())).SetConn(c)
	return client, func() {
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
