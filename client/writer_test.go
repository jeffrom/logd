package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestWriter(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, client := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)
	defer w.Close()
	defer expectServerClose(t, gconf, server)

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(fixture, p) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", fixture, p)
		}
		return protocol.NewClientBatchResponse(gconf, 10, 1)
	})

	writeBatch(t, w, "hi", "hallo", "sup")
	flushBatch(t, w)
}

func TestWriterFillBatch(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	server, client := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)
	defer w.Close()
	defer expectServerClose(t, gconf, server)
	msg := []byte("pretty cool message!")
	buf := newLockedBuffer()

	for i := 0; i < 2; i++ {
		server.Expect(func(p []byte) io.WriterTo {
			buf.Write(p)
			return protocol.NewClientBatchResponse(gconf, 10, 1)
		})
	}

	n := 0
	i := 0
	for n < conf.BatchSize-len(msg) {
		x, err := w.Write([]byte(fmt.Sprintf("%d %s %d", i, msg, i)))
		i++
		n += x
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
	flushBatch(t, w)

	testhelper.CheckGoldenFile("writer.fillbatch", buf.Bytes(), testhelper.Golden)
}

func TestWriterTwoBatches(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	server, client := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)
	defer w.Close()
	defer expectServerClose(t, gconf, server)
	buf := newLockedBuffer()

	f, err := os.Open("testdata/work_of_art.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	n := 0
	for i := 0; i < 3; i++ {
		server.Expect(func(p []byte) io.WriterTo {
			buf.Write(p)
			cr := protocol.NewClientBatchResponse(gconf, uint64(n), 1)
			n += len(p)
			return cr
		})
	}

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		_, err := w.Write(internal.CopyBytes(scanner.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	testhelper.CheckGoldenFile("writer.art", buf.Bytes(), testhelper.Golden)
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

func expectServerClose(t testing.TB, conf *config.Config, server *testhelper.MockServer) {
	server.Expect(func(p []byte) io.WriterTo {
		expect := []byte("CLOSE\r\n")
		if !bytes.Equal(p, expect) {
			t.Fatalf("expected:\n\n\t%q but got:\n\n\t%q", expect, p)
		}
		return protocol.NewClientOKResponse(conf)
	})
}

func confForTimerTest(conf *Config) *Config {
	conf.WaitInterval = 10
	return conf
}

func newTestWriterConn(conf *Config) (net.Conn, *Writer, func()) {
	server, client := net.Pipe()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)

	return server, w, func() {
		w.Close()
		server.Close()
	}
}

type lockedBuffer struct {
	*bytes.Buffer
	mu sync.Mutex
}

func newLockedBuffer() *lockedBuffer {
	return &lockedBuffer{
		Buffer: &bytes.Buffer{},
	}
}

func (b *lockedBuffer) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Read(p)
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Bytes()
}
