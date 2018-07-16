package testhelper

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/jeffrom/logd/internal"
)

// MockServer sets expected requests and responds
type MockServer struct {
	c                 net.Conn
	b                 []byte
	in                chan func([]byte) io.WriterTo
	connInC           chan struct{}
	connRetC          chan net.Conn
	connCreatedC      chan struct{}
	done              chan struct{}
	closeNext         int
	closedExpectation func([]byte) io.WriterTo
}

// NewMockServer returns a new instance of a MockServer
func NewMockServer(c net.Conn) *MockServer {
	s := &MockServer{
		c:            c,
		b:            make([]byte, 1024*64),
		in:           make(chan func([]byte) io.WriterTo, 100),
		connInC:      make(chan struct{}, 10),
		connRetC:     make(chan net.Conn, 10),
		connCreatedC: make(chan struct{}, 10),
		done:         make(chan struct{}),
	}

	go s.loop()
	return s
}

func (s *MockServer) loop() {
	for {
		select {
		case cb := <-s.in:
			s.handleExpectation(cb)
		case <-s.connInC:
			server, client := net.Pipe()
			s.c = server
			log.Printf("new connection created")
			s.connRetC <- client
		case <-s.connCreatedC:
			s.handleExpectation(s.closedExpectation)
		case <-s.done:
			log.Printf("stopping mock server")
			return
			// case <-time.After(200 * time.Millisecond):
			// 	panic("expected a request but didn't get one")
		}
	}
}

func (s *MockServer) handleExpectation(cb func([]byte) io.WriterTo) {
	internal.LogError(s.c.SetReadDeadline(time.Now().Add(50 * time.Millisecond)))
	n, err := s.c.Read(s.b)
	log.Printf("%s: read %d bytes: %q (err: %+v)", s.c.RemoteAddr(), n, s.b[:n], err)
	if err != nil {
		panic(err)
	}

	if s.closeNext > 0 {
		internal.LogError(s.c.Close())
		s.closeNext--
		log.Printf("closing conn to %s (will close %d more)", s.c.RemoteAddr(), s.closeNext)
		s.closedExpectation = cb
		return
	}
	s.closedExpectation = nil

	resp := cb(s.b[:n])
	wrote, err := resp.WriteTo(s.c)
	log.Printf("%s: wrote %d bytes (err: %+v)", s.c.RemoteAddr(), wrote, err)
	if err != nil {
		panic(err)
	}
}

// Expect sets an expectation for a read, and sends a response
func (s *MockServer) Expect(cb func([]byte) io.WriterTo) {
	log.Printf("added request expectation")
	s.in <- cb
}

// Close implements io.Closer
func (s *MockServer) Close() error {
	var err error
	if s.c != nil {
		err = s.c.Close()
	}
	s.done <- struct{}{}
	return err
}

// CloseN closes the connection during the next n requests
func (s *MockServer) CloseN(n int) {
	s.closeNext = n
}

// DialTimeout returns a new net.Pipe client connection with a timeout
func (s *MockServer) DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	var client net.Conn
	s.connInC <- struct{}{}
	select {
	case conn := <-s.connRetC:
		client = conn
	}
	s.connCreatedC <- struct{}{}
	return client, nil
}
