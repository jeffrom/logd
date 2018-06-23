package client

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestBatchV2(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, c, shutdown := newTestConn(conf)
	defer shutdown()

	batch := protocol.NewBatch(gconf)
	batch.Append([]byte("hi"))
	batch.Append([]byte("hallo"))
	batch.Append([]byte("sup"))

	check := goExpectResps(t, conf, server, []expectedRequest{
		{fixture, protocol.NewClientBatchResponseV2(gconf, 10)},
	}...)

	var expected uint64 = 10
	off, err := c.Batch(batch)
	if err != nil {
		t.Fatalf("sending batch: %+v", err)
	}
	if off != expected {
		t.Fatalf("expected offset %d but got %d", expected, off)
	}
	check()

	check = goExpectResps(t, conf, server, []expectedRequest{
		{fixture, protocol.NewClientErrResponseV2(gconf, errors.New("this should be an internal server error"))},
	}...)

	_, err = c.Batch(batch)
	if err != protocol.ErrInternal {
		t.Fatalf("expected %v but got %+v", protocol.ErrInternal, err)
	}
	check()
}

func TestReadV2(t *testing.T) {

	// go expectResp(t, gconf, server, fixture,
	// 	protocol.NewClientErrResponseV2(gconf, protocol.ErrNotFound))

	// _, err = c.Read(x, y)
	// if err != protocol.ErrNotFound {
	// 	t.Fatalf("expected %v but got %+v", protocol.ErrNotFound, err)
	// }

}

func DefaultTestConfig(verbose bool) *Config {
	c := &Config{}
	*c = *DefaultConfig
	c.Verbose = verbose
	return c
}

func goRespond(t *testing.T, conf *Config, server net.Conn) func() {
	c := make(chan struct{})

	go func() {
		b := make([]byte, conf.BatchSize)
		var err error
		var n int
		var read int
		batch := protocol.NewBatch(conf.toGeneralConfig())

		t.Log("waiting for batches")
		for n == 0 && err != nil {
			select {
			case <-c:
				break
			default:
			}
			n, err = server.Read(b[read:])
			read += n
			t.Logf("received %q", b[:read])
			if err != nil {
				panic(err)
			}

			_, err = batch.ReadFrom(bufio.NewReader(bytes.NewReader(b[:read])))
		}
	}()

	return func() {
		c <- struct{}{}
	}
}

func expectResp(t *testing.T, conf *Config, server net.Conn, req []byte, cr *protocol.ClientResponse) {
	b := make([]byte, conf.BatchSize)
	var err error
	var n int
	var read int
	for err == nil {
		// server.SetDeadline(time.Now().Add(time.Duration(conf.ServerTimeout)))
		n, err = server.Read(b[read:])
		// server.SetDeadline(time.Time{})
		read += n
		t.Logf("received %q", b[:read])
		if bytes.Equal(b[:read], req) {
			break
		}
	}
	if err != nil && err != io.EOF {
		log.Panicf("reading result: %+v", err)
	}

	tmpb := &bytes.Buffer{}
	cr.WriteTo(tmpb)
	t.Logf("responding %q", tmpb.Bytes())
	if _, err := cr.WriteTo(server); err != nil {
		panic(err)
	}
}

type expectedRequest struct {
	req  []byte
	resp *protocol.ClientResponse
}

func goExpectResps(t *testing.T, conf *Config, server net.Conn, expects ...expectedRequest) func() {
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, expect := range expects {
			t.Logf("expecting: %q", expect.req)
			expectResp(t, conf, server, expect.req, expect.resp)
		}
	}()

	return func() {
		c := make(chan struct{})
		go func() {
			defer close(c)
			wg.Wait()
		}()

		select {
		case <-c:
		case <-time.After(time.Millisecond * 100):
			panic("timed out")
		}
	}
}

func newTestConn(conf *Config) (net.Conn, *ClientV2, func()) {
	server, client := net.Pipe()
	c := NewClientV2(conf).SetConn(client)

	return server, c, func() {
		c.Close()
		server.Close()
	}
}
