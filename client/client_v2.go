package client

import (
	"bufio"
	"io"
	"net"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// ClientV2 represents a connection to the database
type ClientV2 struct { // nolint: golint
	net.Conn
	conf  *Config
	gconf *config.Config

	readTimeout  time.Duration
	writeTimeout time.Duration

	bw *bufio.Writer
	br *bufio.Reader
	cr *protocol.ClientResponse
}

// NewClientV2 returns a new instance of ClientV2 without a net.Conn
func NewClientV2(conf *Config) *ClientV2 {
	// timeout := time.Duration(conf.ClientTimeout) * time.Millisecond
	gconf := conf.toGeneralConfig()
	c := &ClientV2{
		conf:         conf,
		gconf:        gconf,
		cr:           protocol.NewClientResponse(gconf),
		readTimeout:  conf.getReadTimeout(),
		writeTimeout: conf.getWriteTimeout(),
	}

	return c
}

// DialV2 returns a new instance of Conn
func DialV2(addr string) (*ClientV2, error) {
	return DialConfigV2(addr, DefaultConfig)
}

// DialConfigV2 returns a configured Conn
func DialConfigV2(addr string, conf *Config) (*ClientV2, error) {
	// internal.Debugf(conf, "starting options: %s", conf)
	var conn net.Conn
	var err error

	conn, err = net.Dial("tcp", addr)
	if err != nil {
		if conn != nil {
			internal.IgnoreError(conn.Close())
		}

		return nil, err
	}

	gconf := conf.toGeneralConfig()
	return &ClientV2{
		conf:         conf,
		gconf:        gconf,
		Conn:         conn,
		readTimeout:  conf.getReadTimeout(),
		writeTimeout: conf.getWriteTimeout(),
		br:           bufio.NewReader(conn),
		bw:           bufio.NewWriter(conn),
		cr:           protocol.NewClientResponse(gconf),
	}, nil
}

func (c *ClientV2) reset() {
	c.cr.Reset()
}

// SetConn sets net.Conn for a client.
func (c *ClientV2) SetConn(conn net.Conn) *ClientV2 {
	c.Conn = conn
	if c.bw == nil {
		c.bw = bufio.NewWriter(conn)
	} else {
		c.bw.Reset(conn)
	}
	if c.br == nil {
		c.br = bufio.NewReader(conn)
	} else {
		c.br.Reset(conn)
	}
	return c
}

// Batch sends a batch request and returns the response.
func (c *ClientV2) Batch(batch *protocol.Batch) (uint64, error) {
	internal.Debugf(c.gconf, "%v -> %s", batch, c.Conn.RemoteAddr())
	if _, err := c.send(batch); err != nil {
		return 0, err
	}
	return c.readBatchResponse()
}

// Flush flushes all pending data to the server
func (c *ClientV2) Flush() error {
	if c.bw.Buffered() > 0 {
		internal.Debugf(c.gconf, "client.Flush() (%d bytes)", c.bw.Buffered())
		internal.IgnoreError(c.SetWriteDeadline(time.Now().Add(c.writeTimeout)))
		err := c.bw.Flush()
		internal.Debugf(c.gconf, "client.Flush() complete")
		internal.IgnoreError(c.SetWriteDeadline(time.Time{}))
		return err
	}
	return nil
}

func (c *ClientV2) send(wt io.WriterTo) (int64, error) {
	// c.br.Reset(c.Conn)
	internal.IgnoreError(c.SetWriteDeadline(time.Now().Add(c.writeTimeout)))
	n, err := wt.WriteTo(c.bw)
	if c.handleErr(err) != nil {
		return 0, err
	}
	internal.Debugf(c.gconf, "wrote %d bytes to %s", n, c.Conn.RemoteAddr())

	err = c.Flush()
	if c.handleErr(err) != nil {
		return n, err
	}

	internal.IgnoreError(c.SetWriteDeadline(time.Time{}))
	return n, err
}

func (c *ClientV2) readBatchResponse() (uint64, error) {
	c.cr.Reset()
	internal.IgnoreError(c.SetReadDeadline(time.Now().Add(c.readTimeout)))
	n, err := c.cr.ReadFrom(c.br)
	internal.IgnoreError(c.SetReadDeadline(time.Time{}))
	internal.Debugf(c.gconf, "read %d bytes from %s", n, c.Conn.RemoteAddr())
	c.handleErr(err)
	if err != nil {
		return 0, err
	}
	return c.cr.Offset(), c.cr.Error()
}

func (c *ClientV2) handleErr(err error) error {
	if err == nil {
		return err
	}
	if err == io.EOF {
		internal.DebugfDepth(c.gconf, 1, "%s closed the connection", c.Conn.RemoteAddr())
	} else {
		internal.DebugfDepth(c.gconf, 1, "%+v", err)
	}
	return err
}
