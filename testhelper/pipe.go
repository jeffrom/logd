package testhelper

import (
	"bufio"
	"io"
	"net"
)

type bufferedConn struct {
	net.Conn
	rw *bufio.ReadWriter
}

func newBufferedConn(c net.Conn) *bufferedConn {
	return &bufferedConn{
		rw: bufio.NewReadWriter(
			bufio.NewReaderSize(c, 1024*50),
			bufio.NewWriterSize(c, 1024*50),
		),
		Conn: c,
	}
}

func (c *bufferedConn) Write(p []byte) (int, error) {
	defer c.rw.Flush()
	return c.rw.Write(p)
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.rw.Read(p)
}

func (c *bufferedConn) Flush() error {
	return c.rw.Flush()
}

// Pipe returns a buffered net.Pipe
func Pipe() (*MockServer, net.Conn) {
	server, client := net.Pipe()
	return NewMockServer(newBufferedConn(server)), newBufferedConn(client)
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
