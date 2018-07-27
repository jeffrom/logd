package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestWriter(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, _ := testhelper.Pipe()
	defer server.Close()
	w := NewWriter(conf, "default")
	w.Client.dialer = server
	// TODO this prevents a race but would be better to do connections in a
	// separate goroutine from expectations
	w.ensureConn()
	defer w.Close()
	defer expectServerClose(t, gconf, server)

	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(fixture, p) {
			log.Panicf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", fixture, p)
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
	server, _ := testhelper.Pipe()
	defer server.Close()
	w := NewWriter(conf, "default")
	w.Client.dialer = server
	w.ensureConn()
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
		errs := make(chan error, par)
		// w.ensureConn()

		for j := 0; j < par; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := w.Write([]byte("hallo"))
				errs <- err
			}()
		}

		wg.Wait()
		if err := drainErrs(errs); err != nil {
			t.Fatal(err)
		}

		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWriterFillBatch(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	server, _ := testhelper.Pipe()
	defer server.Close()
	w := NewWriter(conf, "default")
	w.Client.dialer = server
	w.ensureConn()
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
	server, _ := testhelper.Pipe()
	defer server.Close()
	w := NewWriter(conf, "default")
	w.Client.dialer = server
	w.ensureConn()
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

func TestWriterConnectFailure(t *testing.T) {
	// t.Skip("temporarily skipping until reconnect logic is fixed")
	conf := DefaultTestConfig(testing.Verbose())
	conf.ConnRetries = 0
	log.Print(conf)
	gconf := conf.ToGeneralConfig()
	// fixture := testhelper.LoadFixture("batch.small")
	server, client := testhelper.Pipe()
	server.CloseConnN(2)
	client.Close()
	defer server.Close()
	w := NewWriter(conf, "default")
	w.Client.dialer = server
	defer w.Close()

	t.Logf("doing first attempt (should fail)")
	_, err := w.Write([]byte("aw shucks sup"))
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	time.Sleep(10 * time.Millisecond)
	server.Expect(func(p []byte) io.WriterTo {
		return protocol.NewClientBatchResponse(gconf, 10, 1)
	})

	t.Logf("doing second attempt (should succeed)")
	_, err = w.Write([]byte("aw shucks sup"))
	if err != nil {
		t.Fatal("expected nil error but got", err)
	}
	flushBatch(t, w)
}

func TestWriterStatePusher(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, _ := testhelper.Pipe()
	sp := NewMockStatePusher()
	w := NewWriter(conf, "default").WithStateHandler(sp)
	w.Client.dialer = server
	// TODO this prevents a race but would be better to do connections in a
	// separate goroutine from expectations
	w.ensureConn()
	defer w.Close()

	server.Expect(func(p []byte) io.WriterTo {
		return protocol.NewClientBatchResponse(gconf, 10, 1)
	})

	writeBatch(t, w, "idk", "ikr", "yessssss")
	flushBatch(t, w)

	off, err, batch, ok := sp.Next()
	if !ok {
		t.Fatal("expected state to be pushed")
	}
	if off != 10 {
		t.Fatalf("expected pushed offset to be %d but was %d", 10, off)
	}
	if err != nil {
		t.Fatalf("expected no pushed error but got: %+v", err)
	}
	if batch != nil {
		t.Fatalf("expected nil batch but got %+v", batch)
	}

	server.Close()
	writeBatch(t, w, "hi", "hallo", "sup")
	if err := w.Flush(); err == nil {
		t.Fatal("expected error but got none")
	}

	_, err, batch, ok = sp.Next()
	if !ok {
		t.Fatal("expected state to be pushed")
	}

	if err == nil {
		t.Fatal("expected pushed error but got none")
	}
	if batch == nil {
		t.Fatal("expected pushed batch but got none")
	}
	// fmt.Println(batch)
	b := &bytes.Buffer{}
	batch.WriteTo(b)
	if !bytes.Equal(b.Bytes(), fixture) {
		t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q", fixture, b.Bytes())
	}
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
			log.Panicf("expected:\n\n\t%q but got:\n\n\t%q", expect, p)
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

func drainErrs(errs chan error) error {
	for {
		select {
		case err := <-errs:
			if err != nil {
				return err
			}
		default:
			return nil
		}
	}
}
