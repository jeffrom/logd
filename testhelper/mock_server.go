package testhelper

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
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
	closeConnNext     int
	closedExpectation func([]byte) io.WriterTo
	mu                sync.Mutex
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
		log.Print("waiting for event")
		select {
		case cb := <-s.in:
			log.Print("<-in")
			s.handleExpectation(cb)
		case <-s.connInC:
			log.Print("<-connInC")
			s.connRetC <- s.resetConn()
		case <-s.connCreatedC:
			log.Print("<-connCreatedC")
			if s.closedExpectation != nil {
				s.handleExpectation(s.closedExpectation)
			}
		case <-s.done:
			log.Printf("stopping mock server")
			return
			// case <-time.After(200 * time.Millisecond):
			// 	panic("expected a request but didn't get one")
		}
	}
}

func (s *MockServer) resetConn() net.Conn {
	server, client := net.Pipe()
	s.c = server
	log.Printf("new connection created")
	return client
}

func (s *MockServer) handleExpectation(cb func([]byte) io.WriterTo) {
	internal.LogError(s.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond)))
	n, err := s.c.Read(s.b)
	if nerr, ok := err.(*net.OpError); ok && n == 0 && nerr.Temporary() {
		log.Printf("%s: read failed, maybe because of reconnection. err: %+v", s.c.RemoteAddr(), err)
		return
	}

	log.Printf("%s: read %d bytes: %q (err: %+v)", s.c.RemoteAddr(), n, s.b[:n], err)
	if err != nil {
		log.Panicf("expected request but read failed: %+v", err)
	}

	if closeNext := s.checkCloseNext(); closeNext > 0 {
		internal.LogError(s.c.Close())
		s.CloseN(closeNext - 1)
		log.Printf("closing conn to %s (will close %d more)", s.c.RemoteAddr(), s.closeNext)
		s.closedExpectation = cb
		return
	}
	s.closedExpectation = nil

	resp := cb(s.b[:n])
	wrote, err := resp.WriteTo(s.c)

	b := &bytes.Buffer{}
	resp.WriteTo(b)
	log.Printf("%s: wrote %d bytes: %q (err: %+v)", s.c.RemoteAddr(), wrote, b.Bytes(), err)
	// if err != nil {
	// 	panic(err)
	// }
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeNext = n
}

func (s *MockServer) checkCloseNext() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeNext
}

func (s *MockServer) CloseConnN(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeConnNext = n
}

func (s *MockServer) checkCloseConnNext() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeConnNext
}

// DialTimeout returns a new net.Pipe client connection with a timeout
func (s *MockServer) DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	if closeConnNext := s.checkCloseConnNext(); closeConnNext > 0 {
		s.CloseConnN(closeConnNext - 1)
		log.Print("closing connection, remaining closes: ", closeConnNext-1)
		return nil, &net.OpError{
			Op:   "dial-mocktcp",
			Net:  "mocktcp pipe",
			Addr: nil,
			Err:  errors.New("connection failed"),
		}
	}
	log.Print("DialTimeout: making new pipe")

	var client net.Conn
	s.connInC <- struct{}{}

	select {
	case conn := <-s.connRetC:
		client = conn
		log.Print("DialTimeout: got new pipe")
	}
	s.connCreatedC <- struct{}{}
	return client, nil
}
