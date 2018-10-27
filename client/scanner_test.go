package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

// TODO that if the server doesn't send back enough to reach conf.Limit, the
// scanner requests more.

func TestScanner(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	conf.Offset = 0
	conf.Limit = 3
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	s := ScannerForClient(c)
	defer s.Close()
	defer expectServerClose(t, gconf, server)
	s.SetTopic("default")

	expected := []byte(fmt.Sprintf("READ default 0 %d\r\n", conf.Limit))
	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(p, expected) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
		}

		return readOKResponse(gconf, 0, 1, fixture)
	})

	expectedMsgs := [][]byte{
		[]byte("hi"),
		[]byte("hallo"),
		[]byte("sup"),
	}

	for i, expectedMsg := range expectedMsgs {
		ok := s.Scan()
		if !ok {
			t.Fatalf("stopped scanning too early (%d/%d) (err: %+v)", i, len(expectedMsgs), s.Error())
		}

		actual := s.Message().BodyBytes()
		if !bytes.Equal(expectedMsg, actual) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q", expectedMsg, actual)
		}
	}

	if err := s.Error(); err != nil {
		t.Fatalf("scan: %+v", err)
	}

	if s.Scan() {
		t.Fatalf("did not expect to scan another batch")
	}
}

func TestScannerLimit(t *testing.T) {
	nbatches := 5
	conf := DefaultTestConfig(testing.Verbose())
	conf.Offset = 0
	conf.Limit = 15
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	s := ScannerForClient(c)
	defer s.Close()
	defer expectServerClose(t, gconf, server)
	s.SetTopic("default")

	// requested works around an issue where the the value of i in the callback
	// is always -1
	requested := 0
	for i := 0; i < nbatches; i++ {
		server.Expect(func(p []byte) io.WriterTo {
			n := requested
			offset := n * len(fixture)
			expected := []byte(fmt.Sprintf("READ default %d %d\r\n", offset, conf.Limit))
			if !bytes.Equal(p, expected) {
				log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
			}

			// fmt.Println(n, len(fixture), n*len(fixture))
			resp := readOKResponse(gconf, uint64(offset), 1, fixture)

			requested++
			return resp
		})
	}

	expectedMsgs := [][]byte{
		[]byte("hi"),
		[]byte("hallo"),
		[]byte("sup"),
	}
	for i := 0; i < nbatches*len(expectedMsgs); i++ {
		expectedMsg := expectedMsgs[i%3]
		ok := s.Scan()
		if !ok {
			t.Fatalf("stopped scanning too early (%d/%d) (err: %+v)", i, nbatches, s.Error())
		}

		actual := s.Message().BodyBytes()
		if !bytes.Equal(expectedMsg, actual) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q", expectedMsg, actual)
		}
	}

	if err := s.Error(); err != nil {
		t.Fatalf("scan: %+v", err)
	}

	if s.Scan() {
		t.Fatalf("did not expect to scan another batch")
	}
}

func TestScannerReadForever(t *testing.T) {
	nbatches := 5
	conf := DefaultTestConfig(testing.Verbose())
	conf.Offset = 0
	conf.Limit = 3
	conf.ReadForever = true
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	s := ScannerForClient(c)
	defer s.Close()
	defer expectServerClose(t, gconf, server)
	s.SetTopic("default")

	// requested works around an issue where the the value of i in the callback
	// is always -1
	requested := 0
	for i := 0; i < nbatches; i++ {
		server.Expect(func(p []byte) io.WriterTo {
			n := requested
			offset := n * len(fixture)
			expected := []byte(fmt.Sprintf("READ default %d %d\r\n", offset, conf.Limit))
			if !bytes.Equal(p, expected) {
				log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
			}

			// fmt.Println(n, len(fixture), n*len(fixture))
			resp := readOKResponse(gconf, uint64(offset), 1, fixture)

			requested++
			return resp
		})
	}

	expectedMsgs := [][]byte{
		[]byte("hi"),
		[]byte("hallo"),
		[]byte("sup"),
	}
	for i := 0; i < nbatches*len(expectedMsgs); i++ {
		expectedMsg := expectedMsgs[i%3]
		ok := s.Scan()
		if !ok {
			t.Fatalf("stopped scanning too early (%d/%d) (err: %+v)", i, nbatches, s.Error())
		}

		actual := s.Message().BodyBytes()
		if !bytes.Equal(expectedMsg, actual) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q", expectedMsg, actual)
		}
	}

	if err := s.Error(); err != nil {
		t.Fatalf("scan: %+v", err)
	}

	server.Expect(func(p []byte) io.WriterTo {
		offset := nbatches * len(fixture)
		expected := []byte(fmt.Sprintf("READ default %d %d\r\n", offset, conf.Limit))
		if !bytes.Equal(p, expected) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
		}
		return protocol.NewClientErrResponse(gconf, protocol.ErrNotFound)
	})

	// not a great way to test, but all i can think the way polling works now
	conf.ReadForever = false
	s.totalMessagesRead = 0
	if s.Scan() {
		t.Fatalf("did not expect to scan another batch")
	}

	if err := s.Error(); err != protocol.ErrNotFound {
		t.Fatalf("expected error %v but got %+v", protocol.ErrNotFound, err)
	}
}

func TestScannerState(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	conf.Offset = 0
	conf.Limit = 3
	conf.UseTail = true
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	sm := NewMemoryStatePuller(conf)
	s := ScannerForClient(c).
		WithStatePuller(sm)
	defer s.Close()
	defer expectServerClose(t, gconf, server)
	s.SetTopic("default")

	// ensure state is initialized
	if err := s.Complete(nil); err != nil {
		t.Fatal(err)
	}

	var offset uint64

	for i := 0; i < 10; i++ {
		server.Expect(okCallback(gconf, fixture, offset))
		mustScan(t, s, "hi")
		mustComplete(t, s)
		mustScan(t, s, "hallo")
		mustScan(t, s, "sup")
		s.Reset()

		server.Expect(okCallback(gconf, fixture, offset))
		mustScan(t, s, "hallo")
		mustComplete(t, s)
		mustScan(t, s, "sup")
		s.Reset()

		server.Expect(okCallback(gconf, fixture, offset))
		mustScan(t, s, "sup")
		mustComplete(t, s)
		s.Reset()
		offset += uint64(len(fixture))
	}
}

func TestScannerUseTail(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	conf.Offset = 0
	conf.Limit = 3
	conf.UseTail = true
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := New(conf).SetConn(clientConn)
	s := ScannerForClient(c)
	defer s.Close()
	defer expectServerClose(t, gconf, server)
	s.SetTopic("default")

	server.Expect(func(p []byte) io.WriterTo {
		req := protocol.NewRequestConfig(gconf)
		if _, err := req.ReadFrom(bufio.NewReader(bytes.NewReader(p))); err != nil {
			panic(err)
		}
		_, err := protocol.NewTail(gconf).FromRequest(req)
		if err != nil {
			panic(err)
		}

		resp := readOKResponse(gconf, 123, 1, fixture)
		return resp
	})

	if !s.Scan() {
		t.Fatal(s.Error())
	}
}

func mustScan(t *testing.T, s *Scanner, expected string) {
	t.Helper()

	if !s.Scan() {
		t.Fatal("failed to scan:", s.Error())
	}
	// t.Logf("read: %q (expected: %q)", s.Message().BodyBytes(), expected)

	if !bytes.Equal([]byte(expected), s.Message().BodyBytes()) {
		t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q", expected, s.Message().BodyBytes())
	}
}

func mustComplete(t *testing.T, s *Scanner) {
	t.Helper()

	if err := s.Complete(nil); err != nil {
		t.Fatal(err)
	}
}

func okCallback(conf *config.Config, fixture []byte, off uint64) func([]byte) io.WriterTo {
	return func(p []byte) io.WriterTo {
		req := protocol.NewRequestConfig(conf)
		if _, err := req.ReadFrom(bufio.NewReader(bytes.NewReader(p))); err != nil {
			panic(err)
		}
		readreq, err := protocol.NewRead(conf).FromRequest(req)
		if err != nil {
			panic(err)
		}

		if readreq.Offset != off {
			log.Panicf("expected offset %d but got %d", off, readreq.Offset)
		}

		resp := readOKResponse(conf, readreq.Offset, 1, fixture)
		return resp
	}
}
