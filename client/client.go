package client

import (
	"bufio"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Dialer defines an interface for connecting to servers. It can be used for
// mocking in tests.
type Dialer interface {
	DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error)
}

type netDialer struct{}

func (nd *netDialer) DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout(network, addr, timeout)
}

// Client represents a connection to the database
type Client struct { // nolint: golint
	net.Conn
	conf  *Config
	gconf *config.Config

	dialer        Dialer
	readTimeout   time.Duration
	writeTimeout  time.Duration
	retries       int
	retryInterval time.Duration

	bw      *bufio.Writer
	br      *bufio.Reader
	cr      *protocol.ClientResponse
	readreq *protocol.Read
	tailreq *protocol.Tail
	bs      *protocol.BatchScanner

	done chan struct{}
}

// New returns a new instance of Client without a net.Conn
func New(conf *Config) *Client {
	// timeout := time.Duration(conf.ClientTimeout) * time.Millisecond
	gconf := conf.toGeneralConfig()
	c := &Client{
		conf:         conf,
		gconf:        gconf,
		dialer:       &netDialer{},
		readTimeout:  conf.getReadTimeout(),
		writeTimeout: conf.getWriteTimeout(),
		cr:           protocol.NewClientResponse(gconf),
		bs:           protocol.NewBatchScanner(gconf, nil),
		readreq:      protocol.NewRead(gconf),
		tailreq:      protocol.NewTail(gconf),
		done:         make(chan struct{}),
	}

	return c
}

// Dial returns a new instance of Conn
func Dial(addr string) (*Client, error) {
	return DialConfig(addr, DefaultConfig)
}

// DialConfig returns a configured Conn
func DialConfig(addr string, conf *Config) (*Client, error) {
	// internal.Debugf(conf, "starting options: %s", conf)
	c := New(conf)
	if err := c.connect(addr); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) reset() {
	c.cr.Reset()
	c.readreq.Reset()
	c.tailreq.Reset()
	select {
	case <-c.done:
	default:
	}
}

func (c *Client) resetRetries() {
	c.retries = 0
	c.retryInterval = 0
}

func (c *Client) connect(addr string) error {
	conn, err := c.dialer.DialTimeout("tcp", addr, c.conf.Timeout)
	if err != nil {
		internal.IgnoreError(c.conf.Verbose, conn.Close())
		return err
	}
	c.reset()
	c.SetConn(conn)
	return nil
}

// Stop causes any pending blocking operation to return ErrStopped
func (c *Client) Stop() {
	c.done <- struct{}{}
}

// SetConn sets net.Conn for a client.
func (c *Client) SetConn(conn net.Conn) *Client {
	if c.Conn != nil {
		internal.LogError(c.Conn.Close())
	}

	c.Conn = conn
	if c.bw == nil {
		c.bw = bufio.NewWriterSize(conn, c.conf.BatchSize)
	} else {
		c.bw.Reset(conn)
	}
	if c.br == nil {
		c.br = bufio.NewReaderSize(conn, c.conf.BatchSize)
	} else {
		c.br.Reset(conn)
	}
	return c
}

// Batch sends a batch request and returns the response.
func (c *Client) Batch(batch *protocol.Batch) (uint64, error) {
	internal.Debugf(c.gconf, "%v -> %s", batch, c.Conn.RemoteAddr())
	if _, _, err := c.doRequest(batch); err != nil {
		return 0, err
	}
	off, _, err := c.readBatchResponse(batch)
	return off, err
}

// ReadOffset sends a READ request, returning a scanner that can be used to
// iterate over the messages in the response.
func (c *Client) ReadOffset(offset uint64, limit int) (int, *protocol.BatchScanner, error) {
	internal.Debugf(c.gconf, "READ %d %d -> %s", offset, limit, c.Conn.RemoteAddr())
	req := c.readreq
	req.Reset()
	req.Offset = offset
	req.Messages = limit

	if _, _, err := c.doRequest(req); err != nil {
		return 0, nil, err
	}

	respOff, nbatches, err := c.readBatchResponse(req)
	if err != nil {
		return 0, nil, err
	}
	if respOff != offset {
		log.Printf("response offset (%d) did not match request (%d)", respOff, offset)
		return 0, nil, protocol.ErrInternal
	}

	c.bs.Reset(c.br)
	internal.LogError(c.SetReadDeadline(time.Now().Add(c.readTimeout)))
	return nbatches, c.bs, nil
}

// Tail sends a TAIL request, returning the initial offset and a scanner
// starting from the first available batch.
func (c *Client) Tail(limit int) (uint64, int, *protocol.BatchScanner, error) {
	internal.Debugf(c.gconf, "TAIL %d -> %s", limit, c.Conn.RemoteAddr())
	req := c.tailreq
	req.Reset()
	req.Messages = limit

	if _, _, err := c.doRequest(req); err != nil {
		return 0, 0, nil, err
	}

	respOff, nbatches, err := c.readBatchResponse(req)
	if err != nil {
		return 0, 0, nil, err
	}

	c.bs.Reset(c.br)
	internal.LogError(c.SetReadDeadline(time.Now().Add(c.readTimeout)))
	return respOff, nbatches, c.bs, nil
}

// Close sends a CLOSE request and then closes the connection
func (c *Client) Close() error {
	defer func() {
		internal.LogError(c.Conn.Close())
	}()

	closereq := protocol.NewCloseRequest(c.gconf)
	if _, _, err := c.doRequest(closereq); err != nil {
		return err
	}

	if err := c.readCloseResponse(); err != nil {
		return err
	}

	return nil
}

func (c *Client) doRequest(wt io.WriterTo) (int64, int64, error) {
	sent, recv, err := c.do(wt)
	if err != nil {
		return c.retryRequest(wt, sent, recv, err)
	}
	return sent, recv, err
}

func (c *Client) do(wt io.WriterTo) (int64, int64, error) {
	internal.LogError(c.SetWriteDeadline(time.Now().Add(c.writeTimeout)))
	sent, err := wt.WriteTo(c.bw)
	internal.LogError(c.SetWriteDeadline(time.Time{}))
	if err != nil {
		return sent, 0, err
	}

	internal.LogError(c.SetWriteDeadline(time.Now().Add(c.writeTimeout)))
	err = c.flush()
	internal.LogError(c.SetWriteDeadline(time.Time{}))
	internal.Debugf(c.gconf, "wrote %d bytes to %s (err: %v)", sent, c.Conn.RemoteAddr(), err)
	if err != nil {
		return sent, 0, err
	}

	recv, err := c.readClientResponse()
	if err != nil {
		return sent, recv, err
	}
	return sent, recv, nil
}

func (c *Client) retryRequest(wt io.WriterTo, origSent, origRecv int64, err error) (int64, int64, error) {
	if c.conf.ConnRetries == 0 {
		return origSent, origRecv, err
	}
	if !isRetryable(err) {
		return origSent, origRecv, err
	}

	var retryErr error
	var sent int64
	var recv int64
	conn := c.Conn
	for c.conf.ConnRetries < 0 || c.retries < c.conf.ConnRetries {
		if retryErr != nil && !isRetryable(err) {
			break
		}
		c.retries++
		if c.retryInterval == 0 {
			c.retryInterval = c.conf.ConnRetryInterval
		} else {
			next := int(math.Round(float64(c.retryInterval) * c.conf.ConnRetryMultiplier))
			c.retryInterval = time.Duration(next)
		}
		if c.retryInterval > c.conf.ConnRetryMaxInterval {
			c.retryInterval = c.conf.ConnRetryMaxInterval
		}

		select {
		case <-time.After(c.retryInterval):
		case <-c.done:
			return 0, 0, ErrStopped
		}
		log.Printf("retrying after %s (attempt %d)", c.retryInterval, c.retries)

		if conn != nil {
			internal.IgnoreError(c.conf.Verbose, conn.Close())
		}
		retryErr = c.connect(c.conf.Hostport)
		if retryErr != nil {
			continue
		}

		sent, recv, retryErr = c.do(wt)
		if retryErr != nil {
			continue
		}

		c.resetRetries()
		break
	}

	if retryErr != nil {
		return sent, recv, retryErr
	}
	return sent, recv, nil
}

// flush flushes all pending data to the server
func (c *Client) flush() error {
	if c.bw.Buffered() > 0 {
		internal.Debugf(c.gconf, "client.Flush() (%d bytes)", c.bw.Buffered())
		internal.LogError(c.SetWriteDeadline(time.Now().Add(c.writeTimeout)))
		err := c.bw.Flush()
		internal.Debugf(c.gconf, "client.Flush() complete (err: %v)", err)
		internal.LogError(c.SetWriteDeadline(time.Time{}))
		return err
	}
	return nil
}

func (c *Client) readClientResponse() (int64, error) {
	c.cr.Reset()
	internal.LogError(c.SetReadDeadline(time.Now().Add(c.readTimeout)))
	n, err := c.cr.ReadFrom(c.br)
	internal.LogError(c.SetReadDeadline(time.Time{}))
	internal.Debugf(c.gconf, "read %d bytes from %s: %+v (err: %v)", n, c.Conn.RemoteAddr(), c.cr, err)
	return n, c.handleErr(err)
}

func (c *Client) readBatchResponse(wt io.WriterTo) (uint64, int, error) {
	return c.cr.Offset(), c.cr.Batches(), c.cr.Error()
}

func (c *Client) readCloseResponse() error {
	if !c.cr.Ok() {
		return errors.New("close failed")
	}
	return nil
}

func (c *Client) handleErr(err error) error {
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

func isRetryable(err error) bool {
	if err == io.EOF || err == io.ErrClosedPipe {
		return true
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() || nerr.Temporary() {
		return true
	}
	return false
}
