package testhelper

import (
	"io"
	"net"
)

// Pipe returns a buffered net.Pipe
func Pipe() (*MockServer, net.Conn) {
	server, client := net.Pipe()
	return NewMockServer(server), client
}

// WriteFlush writes data and flushes. it will panic on error
func WriteFlush(c net.Conn, p []byte) int {
	n, err := c.Write(p)
	if err != nil {
		panic(err)
	}
	return n
}

// ReadOrPanic reads until p is full, or panics. it wont panic on io.EOF
func ReadOrPanic(c net.Conn, p []byte) int {
	read := len(p)
	for read < len(p) {
		n, err := c.Read(p[read:])
		read += n
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
	}
	return read
}
