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

	config *Config

	pr           *protocolReader
	readTimeout  time.Duration
	pw           *protoWriter
	writeTimeout time.Duration

	state connState

	done chan struct{}
	mu   sync.Mutex

	written int
}

func newServerConn(c net.Conn, config *Config) *conn {
	timeout := time.Duration(time.Duration(config.ServerTimeout) * time.Millisecond)
	conn := &conn{
		config:       config,
		Conn:         c,
		pr:           newProtocolReader(config),
		readTimeout:  timeout,
		pw:           newProtoWriter(c, config),
		writeTimeout: timeout,
		done:         make(chan struct{}),
	}

	return conn
}

func (c *conn) write(bufs ...[]byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	debugf(c.config, "->%s: %q", c.RemoteAddr(), prettybuf(bufs...))

	var n int
	for _, buf := range bufs {
		wrote, err := c.pw.bw.Write(buf)
		n += wrote
		c.written += wrote
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
	// we only care about the channel if we're gracefully shutting down
	select {
	case c.done <- struct{}{}:
	default:
	}
	return err
}

func (c *conn) setWaitForCmdDeadline() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == connStateReading {
		c.SetDeadline(time.Time{})
	} else {
		timeout := time.Duration(c.config.ServerTimeout) * time.Millisecond
		err := c.SetReadDeadline(time.Now().Add(timeout))
		if cerr := handleConnErr(c.config, err, c); cerr != nil {
			return cerr
		}

		c.state = connStateActive
	}

	return nil
}

func prettybuf(bufs ...[]byte) []byte {
	var flat []byte
	limit := 100
	for _, b := range bufs {
		flat = append(flat, b...)
	}
	if len(flat) > limit {
		// flat = flat[:limit-5] + []byte("...") + flat[limit-2:]
		var final []byte
		final = append(final, flat[:limit-5]...)
		final = append(final, []byte("...")...)
		final = append(final, flat[len(flat)-2:]...)
		return final
	}
	return flat
}
