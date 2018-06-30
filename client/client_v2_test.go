package client

import (
	"bytes"
	"errors"
	"io"
	"log"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestBatchWriteV2(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := NewClientV2(conf).SetConn(clientConn)

	var expectedID uint64 = 10

	batch := protocol.NewBatch(gconf)
	batch.Append([]byte("hi"))
	batch.Append([]byte("hallo"))
	batch.Append([]byte("sup"))

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(fixture, p) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", fixture, p)
		}
		return protocol.NewClientBatchResponseV2(gconf, expectedID)
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
		return protocol.NewClientErrResponseV2(gconf, errors.New("this should be an internal server error"))
	})

	_, err = c.Batch(batch)
	if err != protocol.ErrInternal {
		t.Fatalf("expected %v but got %+v", protocol.ErrInternal, err)
	}
}

func TestReadV2(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := NewClientV2(conf).SetConn(clientConn)

	expected := []byte("READV2 10 3\r\n")
	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(p, expected) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
		}

		return readOKResponse(gconf, 10, fixture)
	})

	scanner, err := c.ReadOffset(10, 3)
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
		return protocol.NewClientErrResponseV2(gconf, protocol.ErrNotFound)
	})

	scanner, err = c.ReadOffset(10, 3)
	if err != protocol.ErrNotFound {
		t.Fatalf("expected %v but got %+v", protocol.ErrNotFound, err)
	}
}

func DefaultTestConfig(verbose bool) *Config {
	c := &Config{}
	*c = *DefaultConfig
	c.Verbose = verbose
	// c.BatchSize = 1024 * 10
	return c
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

func readOKResponse(gconf *config.Config, off uint64, batches ...[]byte) io.WriterTo {
	wt := []io.WriterTo{protocol.NewClientBatchResponseV2(gconf, off)}
	for _, b := range batches {
		wt = append(wt, bytes.NewBuffer(b))
	}

	return &multiWriterTo{wt}
}
