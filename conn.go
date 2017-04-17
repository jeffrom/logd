package logd

import (
	"net"
	"sync"
	"time"
)

const termLen = 2

type conn struct {
	net.Conn

	config *Config

	pr           *protoReader
	readTimeout  time.Duration
	pw           *protoWriter
	writeTimeout time.Duration

	mu sync.Mutex
}

func newConn(c net.Conn, conf *Config) *conn {
	conn := &conn{
		config:       conf,
		Conn:         c,
		pr:           newProtoReader(c, conf),
		readTimeout:  time.Duration(500 * time.Millisecond),
		pw:           newProtoWriter(c, conf),
		writeTimeout: time.Duration(500 * time.Millisecond),
	}

	return conn
}

func (c *conn) write(bufs ...[]byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var n int
	for _, buf := range bufs {
		wrote, err := c.pw.bw.Write(buf)
		n += wrote
		if err != nil {
			return n, err
		}
	}

	err := c.pw.bw.Flush()
	return n, err
}
