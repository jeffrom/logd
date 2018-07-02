package server

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/logger"
)

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

// Conn is a wrapped net.Conn
type Conn struct {
	net.Conn

	config *config.Config

	id string

	readTimeout  time.Duration
	writeTimeout time.Duration
	br           *bufio.Reader
	bw           *bufio.Writer

	state connState

	done chan struct{}
	mu   sync.Mutex

	written int
}

func newServerConn(c net.Conn, conf *config.Config) *Conn {
	timeout := time.Duration(conf.ServerTimeout) * time.Millisecond
	conn := &Conn{
		config:       conf,
		id:           newUUID(),
		Conn:         c,
		readTimeout:  timeout,
		br:           bufio.NewReader(c),
		bw:           bufio.NewWriter(c),
		writeTimeout: timeout,
		done:         make(chan struct{}, 10),
	}

	return conn
}

func newUUID() string {
	uuid := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		panic(err)
	}

	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return string(fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]))
}

// sync write. needs to write any pending data on the channel first.
func (c *Conn) write(bufs ...[]byte) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	internal.Debugf(c.config, "->%s: %q", c.RemoteAddr(), internal.Prettybuf(bufs...))

	var n int64
	for _, buf := range bufs {
		wrote, err := c.bw.Write(buf)
		n += int64(wrote)
		c.written += wrote
		if err != nil {
			return n, err
		}
	}

	err := c.Flush()
	return n, err
}

func (c *Conn) Write(p []byte) (int, error) {
	return c.bw.Write(p)
}

// Flush sends all pending data over the connection
func (c *Conn) Flush() error {
	if c.bw.Buffered() > 0 {
		internal.Debugf(c.config, "%s: flush() (%d bytes buffered)", c.RemoteAddr(), c.bw.Buffered())
		return c.bw.Flush()
	}
	return nil
}

func (c *Conn) Read(p []byte) (int, error) {
	return c.br.Read(p)
}

func (c *Conn) readFrom(r io.Reader) (int64, error) {
	internal.Debugf(c.config, "%s: Conn.readFrom(%+v)", c.RemoteAddr(), r)
	if err := c.setWaitForReadFromDeadline(); err != nil {
		return 0, err
	}
	var n int64
	var err error
	if p, ok := r.(*logger.Partition); ok {
		// sendfile optimization
		if tcpConn, ok := c.Conn.(*net.TCPConn); ok {
			n, err = tcpConn.ReadFrom(p.Reader())
		} else {
			n, err = io.Copy(c.Conn, p.Reader())
		}
	} else {
		n, err = io.Copy(c.Conn, r)
	}
	internal.Debugf(c.config, "%s: wrote %d bytes", c.RemoteAddr(), n)
	return n, handleConnErr(c.config, err, c)
}

func (c *Conn) setState(state connState) {
	c.mu.Lock()
	c.state = state
	c.mu.Unlock()
}

func (c *Conn) getState() connState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *Conn) isActive() bool {
	state := c.getState()
	return state == connStateActive || state == connStateReading
}

func (c *Conn) close() error {
	c.setState(connStateClosed)
	err := c.Conn.Close()

	// we only care about the channel if we're gracefully shutting down
	select {
	case c.done <- struct{}{}:
		internal.Debugf(c.config, "sent to done channel")
	default:
		internal.Debugf(c.config, "failed to write to done channel")
	}

	return err
}

func (c *Conn) setWaitForReadFromDeadline() error {
	timeout := time.Duration(c.config.ServerTimeout) * time.Millisecond
	err := c.SetWriteDeadline(time.Now().Add(timeout))
	return handleConnErr(c.config, err, c)
}

func (c *Conn) setWaitForCmdDeadline() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == connStateReading {
		internal.LogError(c.SetDeadline(time.Time{}))
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

func (c *Conn) setWriteDeadline() error {
	return c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
}

var defaultErrResp = []byte("ERR\r\n")

func (c *Conn) sendDefaultError() (int, error) {
	return c.Write(defaultErrResp)
}

func handleConnErr(config *config.Config, err error, conn *Conn) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		internal.Debugf(config, "%s closed the connection", conn.RemoteAddr())
	} else if err, ok := err.(net.Error); ok && err.Timeout() {
		internal.Logf("%s timed out: %s", conn.RemoteAddr(), debug.Stack())
	} else if err != nil {
		conn.setState(connStateFailed)

		log.Printf("error handling connection: %+v", err)
	}
	return err
}
