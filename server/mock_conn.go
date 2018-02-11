package server

import (
	"net"
	"time"
)

type mockConn struct {
	net.Conn
	returnErr error
	readErr   error
	writeErr  error
}

func newMockConn(c net.Conn) *mockConn {
	return &mockConn{Conn: c}
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.returnErr != nil {
		return 0, m.returnErr
	}
	if m.readErr != nil {
		return 0, m.readErr
	}
	return m.Conn.Read(b)
}

func (m *mockConn) failReadWith(err error) {
	m.readErr = err
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.returnErr != nil {
		return 0, m.returnErr
	}
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return m.Conn.Write(b)
}

func (m *mockConn) failWriteWith(err error) {
	m.writeErr = err
}

func (m *mockConn) Close() error {
	if m.returnErr != nil {
		return m.returnErr
	}
	return m.Conn.Close()
}

func (m *mockConn) LocalAddr() net.Addr {
	return m.Conn.LocalAddr()
}

func (m *mockConn) RemoteAddr() net.Addr {
	return m.Conn.RemoteAddr()
}

func (m *mockConn) SetDeadline(t time.Time) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	return m.Conn.SetDeadline(t)
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	return m.Conn.SetReadDeadline(t)
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	return m.Conn.SetWriteDeadline(t)
}
