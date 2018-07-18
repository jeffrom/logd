package client

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log"
	"testing"

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

func TestReconnect(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	c.dialer = server

	var expectedID uint64
	batch := protocol.NewBatch(gconf)
	batch.SetTopic([]byte("default"))
	batch.Append([]byte("hi"))
	batch.Append([]byte("hallo"))
	batch.Append([]byte("sup"))

	server.CloseN(3)
	server.Expect(func(p []byte) io.WriterTo {
		return protocol.NewClientBatchResponse(gconf, expectedID, 1)
	})

	off, err := c.Batch(batch)
	if err != nil {
		t.Fatalf("sending batch: %+v", err)
	}
	if off != expectedID {
		t.Fatalf("expected resp offset %d but got %d", expectedID, off)
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
