package testhelper

import (
	"io"
	"log"
	"net"
	"time"
)

type logger interface {
	Logf(string, ...interface{})
}

// MockServer sets expected requests and responds
type MockServer struct {
	c         *bufferedConn
	b         []byte
	responder func([]byte) io.WriterTo
	respondC  chan func([]byte) io.WriterTo
	in        chan func([]byte) io.WriterTo
	done      chan struct{}
}

// NewMockServer returns a new instance of a MockServer
func NewMockServer(c *bufferedConn) *MockServer {
	s := &MockServer{
		c:        c,
		b:        make([]byte, 1024*64),
		in:       make(chan func([]byte) io.WriterTo, 10),
		respondC: make(chan func([]byte) io.WriterTo, 10),
		done:     make(chan struct{}),
	}

	go s.loop()
	return s
}

func (s *MockServer) loop() {
	for {
		select {
		case cb := <-s.respondC:
			s.responder = cb
			s.handleRespond(cb)
		case cb := <-s.in:
			s.handleExpectation(cb)
		case <-s.done:
			log.Printf("stopping mock server")
			return
		case <-time.After(200 * time.Millisecond):
			panic("expected a request but didn't get one")
		}
	}
}

func (s *MockServer) handleRespond(cb func([]byte) io.WriterTo) {
	defer func() {
		time.Sleep(25 * time.Millisecond)
		s.respondC <- cb
	}()

	read := 0
	s.c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, err := s.c.Read(s.b[read:])
	if err == io.ErrClosedPipe {
		return
	}
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return
	}

	for err == nil {
		s.c.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		n, err = s.c.Read(s.b[read:])
		read += n
		if err == io.ErrClosedPipe {
			return
		}
		if terr, ok := err.(net.Error); ok && terr.Timeout() {
			err = nil
			break
		}
	}

	log.Printf("%s: read %d bytes: %q (err: %+v)", s.c.RemoteAddr(), read, s.b[:n], err)
	if err != nil {
		panic(err)
	}
	resp := cb(s.b[:n])
	wrote, err := resp.WriteTo(s.c)
	if err != nil {
		panic(err)
	}
	log.Printf("%s: wrote %d bytes (err: %+v)", s.c.RemoteAddr(), wrote, err)
	err = s.c.Flush()
	if err != nil {
		panic(err)
	}
}

// Respond will respond using cb until the server is stopped
func (s *MockServer) Respond(cb func([]byte) io.WriterTo) {
	s.respondC <- cb
}

func (s *MockServer) handleExpectation(cb func([]byte) io.WriterTo) {
	s.c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, err := s.c.Read(s.b)
	log.Printf("%s: read %d bytes: %q (err: %+v)", s.c.RemoteAddr(), n, s.b[:n], err)
	if err != nil {
		panic(err)
	}

	resp := cb(s.b[:n])
	wrote, err := resp.WriteTo(s.c)
	log.Printf("%s: wrote %d bytes (err: %+v)", s.c.RemoteAddr(), wrote, err)
	if err != nil {
		panic(err)
	}

	err = s.c.Flush()
	if err != nil {
		panic(err)
	}
}

// Expect sets an expectation for a read, and sends a response
func (s *MockServer) Expect(cb func([]byte) io.WriterTo) {
	if s.responder != nil {
		panic("don't use Expect when Respond has been called")
	}
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
