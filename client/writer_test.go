package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestWriter(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, client := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)
	w.SetTopic("default")
	defer w.Close()
	defer expectServerClose(t, gconf, server)

	server.Expect(func(p []byte) io.WriterTo {
		// fmt.Println(string(p))
		if !bytes.Equal(fixture, p) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", fixture, p)
		}
		return protocol.NewClientBatchResponse(gconf, 10, 1)
	})

	writeBatch(t, w, "hi", "hallo", "sup")
	flushBatch(t, w)
}

func TestWriterConcurrent(t *testing.T) {
	// t.SkipNow()
	conf := DefaultTestConfig(testing.Verbose())
	conf.BatchSize = 1024 * 1024
	gconf := conf.ToGeneralConfig()
	server, client := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)
	w.dialer = server
	w.SetTopic("default")
	defer w.Close()
	defer expectServerClose(t, gconf, server)

	var offset uint64
	par := 10
	n := 100
	for i := 0; i < n; i++ {
		if i%10 == 0 {
			t.Logf("%d times", i)
		}

		server.Expect(func(p []byte) io.WriterTo {
			// t.Logf("%q", p)
			off := offset
			atomic.AddUint64(&offset, uint64(len(p)))
			return protocol.NewClientBatchResponse(gconf, off, 1)
		})

		wg := sync.WaitGroup{}

		for j := 0; j < par; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				writeBatch(t, w, "hi", "hallo", "sup")
			}()
		}

		wg.Wait()
		flushBatch(t, w)
	}
}

func TestWriterFillBatch(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	server, client := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)
	w.SetTopic("default")
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
	gconf := conf.ToGeneralConfig()
	server, client := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(client)
	w := WriterForClient(c)
	w.SetTopic("default")
	defer w.Close()
	defer expectServerClose(t, gconf, server)
	buf := newLockedBuffer()

	f, err := os.Open("testdata/work_of_art.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var n uint64
	for i := 0; i < 3; i++ {
		server.Expect(func(p []byte) io.WriterTo {
			buf.Write(p)
			cr := protocol.NewClientBatchResponse(gconf, uint64(n), 1)
			atomic.AddUint64(&n, uint64(len(p)))
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
		// t.Logf("write: %q", msg)
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
