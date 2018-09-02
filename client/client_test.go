package client

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log"
	"testing"
	"testing/iotest"

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

func TestBatchWrite(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	// defer expectServerClose(t, gconf, server)
	c := New(conf).SetConn(clientConn)

	var expectedID uint64 = 10

	batch := protocol.NewBatch(gconf)
	batch.SetTopic([]byte("default"))
	batch.Append([]byte("hi"))
	batch.Append([]byte("hallo"))
	batch.Append([]byte("sup"))

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(fixture, p) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", fixture, p)
		}
		return protocol.NewClientBatchResponse(gconf, expectedID, 1)
	})

	off, err := c.Batch(batch)
	if err != nil {
		t.Fatalf("sending batch: %+v", err)
	}
	if off != expectedID {
		t.Fatalf("expected resp offset %d but got %d", expectedID, off)
	}

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(fixture, p) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", fixture, p)
		}
		return protocol.NewClientErrResponse(gconf, errors.New("this should be an internal server error"))
	})

	_, err = c.Batch(batch)
	if err != protocol.ErrInternal {
		t.Fatalf("expected %v but got %+v", protocol.ErrInternal, err)
	}
}

func TestBatchEmpty(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	batch := protocol.NewBatch(gconf)
	_, err := c.Batch(batch)
	if err != ErrEmptyBatch {
		t.Fatalf("expected %v but got %+v", ErrEmptyBatch, err)
	}
}

func TestBatchErrors(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	conf.ConnRetries = 0
	gconf := conf.ToGeneralConfig()
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	batch := protocol.NewBatch(gconf)
	batch.SetTopic([]byte("default"))
	batch.Append([]byte("hi"))
	batch.Append([]byte("hallo"))
	batch.Append([]byte("sup"))

	_, err := c.Batch(batch)
	if err == nil {
		t.Fatal("expected err but got", err)
	}

	c.setWriter(iotest.TruncateWriter(c.w, 3))
	_, err = c.Batch(batch)
	if err == nil {
		t.Fatal("expected err but got", err)
	}
}

func TestRead(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	expected := []byte("READ default 10 3\r\n")
	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(p, expected) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
		}

		return readOKResponse(gconf, 10, 1, fixture)
	})

	_, scanner, err := c.ReadOffset([]byte("default"), 10, 3)
	if err != nil {
		t.Fatalf("ReadOffset: %+v", err)
	}

	ok := scanner.Scan()
	if !ok {
		t.Fatalf("failed to scan: %+v", scanner.Error())
	}

	batch := scanner.Batch()
	t.Logf("read %+v", batch)
	if serr := scanner.Error(); serr != nil {
		t.Fatalf("scanner: %+v", serr)
	}

	server.Expect(func(p []byte) io.WriterTo {
		return protocol.NewClientErrResponse(gconf, protocol.ErrNotFound)
	})

	_, scanner, err = c.ReadOffset([]byte("default"), 10, 3)
	if err != protocol.ErrNotFound {
		t.Fatalf("expected %v but got %+v", protocol.ErrNotFound, err)
	}
}

func TestReadErrors(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	conf.ConnRetries = 0
	// gconf := conf.ToGeneralConfig()
	// fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	_, scanner, err := c.ReadOffset([]byte("default"), 10, 3)
	if err == nil {
		t.Fatalf("expected not nil error but got %v", err)
	}
	if scanner != nil {
		t.Fatal("scanner was not nil")
	}

	c.setWriter(iotest.TruncateWriter(c.w, 3))

	_, scanner, err = c.ReadOffset([]byte("default"), 10, 3)
	if err == nil {
		t.Fatalf("expected not nil error but got %v", err)
	}
	if scanner != nil {
		t.Fatal("scanner was not nil")
	}
}

func TestReadIncorrectRespOffset(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	conf.ConnRetries = 0
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	server.Expect(func(p []byte) io.WriterTo {
		return readOKResponse(gconf, 0, 1, fixture)
	})

	_, scanner, err := c.ReadOffset([]byte("default"), 10, 3)
	if err == nil {
		t.Fatalf("expected error but got %v", err)
	}
	if scanner != nil {
		t.Fatal("scanner was not nil")
	}
}

func TestReadRetryAndFail(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	c.setWriter(iotest.TruncateWriter(c.w, 3))

	_, scanner, err := c.ReadOffset([]byte("default"), 0, 3)
	if err == nil {
		t.Fatalf("expected non-nil error but got %v", err)
	}
	if scanner != nil {
		t.Fatal("scanner was not nil")
	}
}

func TestReadDisruption(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	c.setReader(iotest.HalfReader(c.r))

	server.Expect(func(p []byte) io.WriterTo {
		return readOKResponse(gconf, 10, 1, fixture)
	})

	batches, scanner, err := c.ReadOffset([]byte("default"), 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if scanner == nil {
		t.Fatal("scanner was nil")
	}
	if batches != 1 {
		t.Fatal("expected 1 batch but got", batches)
	}
}

func TestReadTimeout(t *testing.T) {
	t.Skip("not implemented")
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	c.setReader(iotest.TimeoutReader(c.r))

	server.Expect(func(p []byte) io.WriterTo {
		return readOKResponse(gconf, 10, 1, fixture)
	})

	batches, scanner, err := c.ReadOffset([]byte("default"), 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if scanner == nil {
		t.Fatal("scanner was nil")
	}
	if batches != 1 {
		t.Fatal("expected 1 batch but got", batches)
	}

	server.Expect(func(p []byte) io.WriterTo {
		return readOKResponse(gconf, 10, 1, fixture)
	})

	batches, scanner, err = c.ReadOffset([]byte("default"), 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if scanner == nil {
		t.Fatal("scanner was nil")
	}
	if batches != 1 {
		t.Fatal("expected 1 batch but got", batches)
	}
}

type closedWriter struct{}

func (w closedWriter) Write(p []byte) (int, error) {
	return 0, io.EOF
}

func TestTail(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	expected := []byte("TAIL default 3\r\n")
	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(p, expected) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
		}

		return readOKResponse(gconf, 10, 1, fixture)
	})

	_, _, scanner, err := c.Tail([]byte("default"), 3)
	if err != nil {
		t.Fatalf("Tail: %+v", err)
	}

	ok := scanner.Scan()
	if !ok {
		t.Fatalf("failed to scan: %+v", scanner.Error())
	}

	batch := scanner.Batch()
	t.Logf("read %+v", batch)
	if serr := scanner.Error(); serr != nil {
		t.Fatalf("scanner: %+v", serr)
	}
}

func TestClose(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(p, []byte("CLOSE\r\n")) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", "CLOSE\r\n", p)
		}
		return protocol.NewClientOKResponse(gconf)
	})

	err := c.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestConfig(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)

	respb := &bytes.Buffer{}
	confResp := protocol.NewConfigResponse(gconf)
	confResp.WriteTo(respb)

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(p, []byte("CONFIG\r\n")) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", "CONFIG\r\n", p)
		}
		return protocol.NewClientMultiResponse(gconf, respb.Bytes())
	})

	rconf, err := c.Config()
	if err != nil {
		t.Fatal(err)
	}

	if rconf.Host != gconf.Host {
		t.Errorf("expected %q but got %q", gconf.Host, rconf.Host)
	}
	if rconf.Timeout != gconf.Timeout {
		t.Errorf("expected %q but got %q", gconf.Timeout.String(), rconf.Timeout.String())
	}
	if rconf.IdleTimeout != gconf.IdleTimeout {
		t.Errorf("expected %q but got %q", gconf.IdleTimeout.String(), rconf.IdleTimeout.String())
	}
	if rconf.MaxBatchSize != gconf.MaxBatchSize {
		t.Errorf("expected %d but got %d", gconf.MaxBatchSize, rconf.MaxBatchSize)
	}
}

func TestReconnect(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	c.dialer = server

	server.CloseN(3)
	server.Expect(func(p []byte) io.WriterTo {
		return readOKResponse(gconf, 10, 1, fixture)
	})

	batches, scanner, err := c.ReadOffset([]byte("default"), 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if scanner == nil {
		t.Fatal("scanner was nil")
	}
	if batches != 1 {
		t.Fatal("expected 1 batch but got", batches)
	}
}

type multiWriterTo struct {
	wt []io.WriterTo
}

// WriteTo implements io.WriterTo
func (wt *multiWriterTo) WriteTo(w io.Writer) (int64, error) {
	b := &bytes.Buffer{}
	for _, wr := range wt.wt {
		_, err := wr.WriteTo(b)
		if err != nil {
			return 0, nil
		}
	}
	return b.WriteTo(w)
}

func readOKResponse(gconf *config.Config, off uint64, nbatches int, batches ...[]byte) io.WriterTo {
	wt := []io.WriterTo{protocol.NewClientBatchResponse(gconf, off, nbatches)}
	for _, b := range batches {
		wt = append(wt, bytes.NewBuffer(b))
	}

	return &multiWriterTo{wt}
}
