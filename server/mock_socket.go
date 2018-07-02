package server

import (
	"net"

	"github.com/jeffrom/logd/config"
)

// MockSocket is a socket server using a net.Pipe internally
type MockSocket struct {
	*Socket
	ml *mockListener
}

// NewMockSocket returns a new instance of a mock socket server
func NewMockSocket(conf *config.Config) *MockSocket {
	conf.Hostport = ""
	s := &MockSocket{
		Socket: NewSocket(conf.Hostport, conf),
		ml:     newMockListener(conf),
	}
	s.Socket.ln = s.ml
	return s
}

// Dial returns a net.Pipe client connection
func (s *MockSocket) Dial() (net.Conn, error) {
	return s.ml.Dial("")
}
