package server

import (
	"errors"
	"net"
	"sync/atomic"

	"github.com/jeffrom/logd/config"
)

type mockListener struct {
	conf   *config.Config
	closeC chan int
	connC  chan net.Conn
	closed uint32
}

func newMockListener(conf *config.Config) *mockListener {
	l := &mockListener{
		conf:   conf,
		closeC: make(chan int),
		connC:  make(chan net.Conn),
	}
	return l
}

func (l *mockListener) Close() error {
	if atomic.CompareAndSwapUint32(&l.closed, 0, 1) {
		// receive from closed channel returns the 0 value immediately
		close(l.closeC)
	}
	return nil
}

func (l *mockListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connC:
		return c, nil
	case <-l.closeC:
		return nil, errors.New("listener closed")
	}
}

func (l *mockListener) Addr() net.Addr {
	return mockAddr(0)
}

func (l *mockListener) Dial(addr string) (net.Conn, error) {
	select {
	case <-l.closeC:
		return nil, errors.New("listener closed")
	default:
	}

	server, client := net.Pipe()
	l.connC <- server
	return client, nil
}

type mockAddr int

func (a mockAddr) Network() string {
	return "mock"
}

func (a mockAddr) String() string {
	return "local mock"
}
