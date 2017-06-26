package logd

import (
	"fmt"
	"net"
	"sync"
	"time"
)

const termLen = 2

type connState uint8

const (
	_ connState = iota

	// connection hasn't been used yet.
	connStateInactive

	// connection is currently handling a command.
	connStateActive

	// connection has been manually closed.
	connStateClosed

	// connection had an error.
	connStateFailed

	// connection is handling a subscription.
	connStateReading
)

func (cs connState) String() string {
	switch cs {
	case connStateInactive:
		return "INACTIVE"
	case connStateActive:
		return "ACTIVE"
	case connStateClosed:
		return "CLOSED"
	case connStateFailed:
		return "FAILED"
	case connStateReading:
		return "READING"
	}
	return fmt.Sprintf("UNKNOWN(%+v)", uint8(cs))
}

type conn struct {
	net.Conn

	config *ServerConfig

	pr           *protoReader
	readTimeout  time.Duration
	pw           *protoWriter
	writeTimeout time.Duration

	state connState

	done chan struct{}
	mu   sync.Mutex
}

func newConn(c net.Conn, conf *ServerConfig) *conn {
	conn := &conn{
		config:       conf,
		Conn:         c,
		pr:           newProtoReader(c, conf),
		readTimeout:  time.Duration(500 * time.Millisecond),
		pw:           newProtoWriter(c, conf),
		writeTimeout: time.Duration(500 * time.Millisecond),
		done:         make(chan struct{}),
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

func (c *conn) setState(state connState) {
	c.mu.Lock()
	c.state = state
	c.mu.Unlock()
}

func (c *conn) getState() connState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *conn) isActive() bool {
	state := c.getState()
	return state == connStateActive || state == connStateReading
}

func (c *conn) close() error {
	c.setState(connStateClosed)
	err := c.Conn.Close()
	c.done <- struct{}{}
	return err
}
