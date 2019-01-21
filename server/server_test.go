package server

import (
	"bufio"
	"bytes"
	"flag"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logd"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
	"github.com/jeffrom/logd/transport"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

func NewTestServer(conf *config.Config) *Socket {
	srv := NewSocket("127.0.0.1:0", conf)
	return srv
}

func expectClose(rh *transport.MockRequestHandler) {
	rh.Expect(func(req *protocol.Request) *protocol.Response {
		conf := testhelper.DefaultConfig(testing.Verbose())
		resp := protocol.NewResponseConfig(conf)
		cr := protocol.NewClientOKResponse(conf)
		req.WriteResponse(resp, cr)
		return resp
	})
}

func CloseTestServer(t testing.TB, srv *Socket, rh *transport.MockRequestHandler) {
	if rh != nil {
		if err := rh.Stop(); err != nil {
			t.Fatal(err)
		}
	}

	if err := srv.Stop(); err != nil {
		t.Fatal(err)
	}

	conns := srv.Conns()
	if len(conns) > 0 {
		for conn := range srv.conns {
			t.Logf("Leftover connection: %s", conn.RemoteAddr())
		}
		t.Fatalf("Leftover connections after server shutdown complete")
	}
}

func TestLifecycle(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	srv := NewTestServer(conf)

	srv.GoServe()
	if err := srv.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	srv := NewTestServer(conf)
	rh := transport.NewMockRequestHandler(conf)
	srv.SetHandler(rh)
	srv.GoServe()

	c, err := logd.Dial(srv.ListenAddr().String())
	if err != nil {
		t.Fatal(err)
	}

	expectClose(rh)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	CloseTestServer(t, srv, rh)
}

func TestBatch(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	srv := NewTestServer(conf)
	rh := transport.NewMockRequestHandler(conf)
	srv.SetHandler(rh)
	srv.GoServe()
	defer CloseTestServer(t, srv, rh)

	c, err := logd.Dial(srv.ListenAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer expectServerClientClose(t, rh, c)

	fixture := testhelper.LoadFixture("batch.small")
	batch := protocol.NewBatch(conf)
	br := bufio.NewReader(bytes.NewBuffer(fixture))
	if _, err := batch.ReadFrom(br); err != nil {
		t.Fatal(err)
	}
	t.Log(batch)

	rh.Expect(func(req *protocol.Request) *protocol.Response {
		resp := protocol.NewResponseConfig(conf)
		cr := protocol.NewClientBatchResponse(conf, 0, 1)
		req.WriteResponse(resp, cr)
		return resp
	})

	if _, err := c.Batch(batch); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidBatch(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	srv := NewTestServer(conf)
	rh := transport.NewMockRequestHandler(conf)
	srv.SetHandler(rh)
	srv.GoServe()
	defer CloseTestServer(t, srv, rh)

	c, err := logd.Dial(srv.ListenAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	// TODO should the connection be closed?
	// defer expectServerClientClose(t, rh, c)

	fixture := testhelper.LoadFixture("batch.badcrc")
	batch := protocol.NewBatch(conf)
	br := bufio.NewReader(bytes.NewBuffer(fixture))
	if _, err := batch.ReadFrom(br); err != nil {
		t.Fatal(err)
	}
	t.Log(batch)

	rh.Expect(func(req *protocol.Request) *protocol.Response {
		panic("should not have gotten to handler")
		resp := protocol.NewResponseConfig(conf)
		cr := protocol.NewClientBatchResponse(conf, 0, 1)
		req.WriteResponse(resp, cr)
		return resp
	})

	if _, err := c.Batch(batch); err == nil {
		t.Fatal("expected error but got none")
	}
}

func TestFailedRequest(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	srv := NewTestServer(conf)
	rh := transport.NewMockRequestHandler(conf)
	srv.SetHandler(rh)
	srv.GoServe()
	defer CloseTestServer(t, srv, rh)

	c, err := logd.Dial(srv.ListenAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer expectServerClientClose(t, rh, c)

	fixture := testhelper.LoadFixture("batch.small")
	batch := protocol.NewBatch(conf)
	br := bufio.NewReader(bytes.NewBuffer(fixture))
	if _, err := batch.ReadFrom(br); err != nil {
		t.Fatal(err)
	}
	t.Log(batch)

	rh.Expect(func(req *protocol.Request) *protocol.Response {
		resp := protocol.NewResponseConfig(conf)
		return resp
	})

	if _, err := c.Batch(batch); err == nil {
		t.Fatal("expected error but got none")
	}

	rh.Expect(func(req *protocol.Request) *protocol.Response {
		return nil
	})

	if _, err := c.Batch(batch); err == nil {
		t.Fatal("expected error but got none")
	}
}

func expectServerClientClose(t testing.TB, rh *transport.MockRequestHandler, c *logd.Client) {
	expectClose(rh)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
