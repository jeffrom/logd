package logd

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func TestReplicationProtocolChunks(t *testing.T) {
	listenChan := make(chan net.Listener)
	connChan := make(chan net.Conn)
	laddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Unexpected error: %q", err)
	}

	go func() {
		l, lerr := net.ListenTCP("tcp", laddr)
		if lerr != nil {
			t.Fatalf("Unexpected error: %q", lerr)
		}

		listenChan <- l

		c, lerr := l.Accept()
		if lerr != nil {
			t.Fatalf("Unexpected error: %q", lerr)
		}
		connChan <- c
	}()

	l := <-listenChan

	clientConn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("Unexpected error: %q", err)
	}

	var serverConn net.Conn
	select {
	case c := <-connChan:
		serverConn = c
	case <-time.After(time.Second):
		t.Fatalf("Timed out waiting for connection")
	}

	replica := newLogReplicaServer(serverConn.(*net.TCPConn))
	scanner := newLogReplicaScanner(clientConn)

	b := []byte("Hey i'm a cool chunk")
	rdr := bytes.NewBuffer(b)
	t.Logf("sending %q", rdr.Bytes())
	_, err = replica.sendChunks(rdr, int64(len(b)))
	if err != nil {
		t.Fatalf("Unexpected error sending chunk: %q", err)
	}

	serverConn.Close()

	scannedOne := false
	for scanner.Scan() {
		scannedOne = true
		t.Logf("buffer: %q", scanner.Buffer())
	}

	if scanner.Err() != nil && scanner.Err() != io.EOF {
		t.Fatalf("Unexpected error: %q", scanner.Err())
	}
	if !scannedOne {
		t.Fatal("failed to scan a single chunk")
	}
}
