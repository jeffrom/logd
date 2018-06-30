package client

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestWriterV2(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, client := testhelper.Pipe()
	defer server.Close()
	c := NewClientV2(conf).SetConn(client)
	w := WriterForClientV2(c)

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(fixture, p) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", fixture, p)
		}
		return protocol.NewClientBatchResponseV2(gconf, 10)
	})

	writeBatch(t, w, "hi", "hallo", "sup")
	flushBatch(t, w)
}

func TestWriterFillBatchV2(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	server, client := testhelper.Pipe()
	defer server.Close()
	c := NewClientV2(conf).SetConn(client)
	w := WriterForClientV2(c)
	msg := []byte("pretty cool message!")

	var read uint64
	server.Respond(func(p []byte) io.WriterTo {
		read += uint64(len(p))
		return protocol.NewClientBatchResponseV2(gconf, read-uint64(len(p)))
	})

	n := 0
	for n < conf.BatchSize-len(msg) {
		x, err := w.Write(msg)
		n += x
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
}

func writeBatch(t *testing.T, w *Writer, msgs ...string) {
	for _, msg := range msgs {
		t.Logf("write: %q", msg)
		n, err := w.Write([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		if n != len(msg) {
			t.Fatalf("expected to write %d bytes (%q), but wrote %d", len(msg), msg, n)
		}
	}
}

func flushBatch(t *testing.T, w *Writer) {
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func confForTimerTest(conf *Config) *Config {
	conf.WaitInterval = 10
	return conf
}

func newTestWriterConn(conf *Config) (net.Conn, *Writer, func()) {
	server, client := net.Pipe()
	c := NewClientV2(conf).SetConn(client)
	w := WriterForClientV2(c)

	return server, w, func() {
		w.Close()
		server.Close()
	}
}
