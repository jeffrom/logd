package client

import (
	"bufio"
	"bytes"
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

// ErrEmptyBatch is returned when an empty batch write is attempted.
var ErrEmptyBatch = errors.New("attempted to send an empty batch")

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
	conf     *Config
	gconf    *config.Config
	hostport string

	dialer        Dialer
	readTimeout   time.Duration
	writeTimeout  time.Duration
	retries       int
	retryInterval time.Duration

	w       io.Writer
	r       io.Reader
	closer  io.Closer
	bw      *bufio.Writer
	br      *bufio.Reader
	sw      io.Writer
	sr      io.Reader
	scloser io.Closer

	// temporarily cache batches when scanning
	batch       *protocol.Batch
	batchbr     *bufio.Reader
	batchbuf    *bytes.Buffer
	rawbatchbuf *bytes.Buffer

	// can cache these here since client should not be used concurrently
	cr      *protocol.ClientResponse
	readreq *protocol.Read
	tailreq *protocol.Tail
	bs      *protocol.BatchScanner

	done chan struct{}
}

// New returns a new instance of Client without a net.Conn
func New(conf *Config) *Client {
	// timeout := time.Duration(conf.ClientTimeout) * time.Millisecond
	gconf := conf.ToGeneralConfig()
	c := &Client{
		conf:         conf,
		gconf:        gconf,
		hostport:     conf.Hostport,
		dialer:       &netDialer{},
		readTimeout:  conf.getReadTimeout(),
		writeTimeout: conf.getWriteTimeout(),
		cr:           protocol.NewClientResponseConfig(gconf),
		bs:           protocol.NewBatchScanner(gconf, nil),
		readreq:      protocol.NewRead(gconf),
		tailreq:      protocol.NewTail(gconf),
		done:         make(chan struct{}),
		batch:        protocol.NewBatch(gconf),
		batchbuf:     &bytes.Buffer{},
		rawbatchbuf:  &bytes.Buffer{},
		batchbr:      bufio.NewReaderSize(nil, conf.BatchSize),
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
	c.hostport = addr
	if err := c.connect(addr); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) reset() {
	c.cr.Reset()
	c.readreq.Reset()
	c.tailreq.Reset()
	c.unsetConn()
	c.batch.Reset()
	c.batchbuf.Reset()
	c.rawbatchbuf.Reset()
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
	internal.Debugf(c.gconf, "connecting to %s", addr)
	conn, err := c.dialer.DialTimeout("tcp", addr, c.conf.Timeout)
	if err != nil {
		if conn != nil {
			internal.IgnoreError(c.conf.Verbose, conn.Close())
		}
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
	if c.closer != nil {
		internal.LogError(c.closer.Close())
	}

	c.Conn = conn
	c.setRW(conn)

	return c
}

func (c *Client) setRW(rw io.ReadWriteCloser) {
	if c.sw == nil {
		c.w = rw
		c.bw = bufio.NewWriterSize(rw, c.conf.BatchSize)
	}

	if c.sr == nil {
		c.r = rw
		c.br = bufio.NewReaderSize(rw, c.conf.BatchSize)
	}

	if c.scloser == nil {
		c.closer = rw
	}
}

func (c *Client) unsetConn() {
	c.Conn = nil
	c.w = nil
	c.sr = nil
	c.r = nil
	c.sr = nil
	c.closer = nil
	c.scloser = nil
}

// setWriter sets the client writer. for testing purposes
func (c *Client) setWriter(w io.Writer) {
	c.sw = w
	c.w = w
	c.bw = bufio.NewWriterSize(w, c.conf.BatchSize)
}

// setReader sets the client reader. for testing purposes
func (c *Client) setReader(r io.Reader) {
	c.sr = r
	c.r = r
	c.br = bufio.NewReaderSize(r, c.conf.BatchSize)
}

// setCloser sets the client closer. for testing purposes
func (c *Client) setCloser(closer io.Closer) {
	c.scloser = closer
	c.closer = closer
}

// unsetSticky unsets any previously set client reader, writer, and closer
func (c *Client) unsetSticky() {
	c.sw = nil
	c.sr = nil
	c.scloser = nil
}

// Batch sends a BATCH request and returns the response. Batch does not retry.
// If you want reconnect functionality, use a Writer
func (c *Client) Batch(batch *protocol.Batch) (uint64, error) {
	if batch.Empty() {
		return 0, ErrEmptyBatch
	}

	// TODO we don't retry BATCH requests. We probably should, but there's an
	// async retry loop the writer uses. Should probably be possible to
	// configure sync (client-level) retries with writer's async retries).
	internal.Debugf(c.gconf, "%v -> %s", batch, c.RemoteAddr())
	if _, _, err := c.do(batch); err != nil {
		return 0, err
	}

	off, _, err := c.readBatchResponse()
	return off, err
}

// Batch sends a BATCH request with a raw batch
func (c *Client) BatchRaw(b []byte) (uint64, error) {
	internal.Debugf(c.gconf, "%q -> %s", b, c.RemoteAddr())

	c.rawbatchbuf.Reset()
	if _, err := c.rawbatchbuf.Write(b); err != nil {
		return 0, err
	}

	// TODO same as Batch retries todo above
	if _, _, err := c.do(c.rawbatchbuf); err != nil {
		return 0, err
	}

	off, _, err := c.readBatchResponse()
	return off, err
}

func (c *Client) readBatches(nbatches int, r *bufio.Reader) (int64, error) {
	var total int64
	var n int64
	var err error
	for i := 0; i < nbatches; i++ {
		c.batch.Reset()
		n, err = c.batch.ReadFrom(r)
		total += n
		if err != nil {
			return total, err
		}

		if _, err := c.batch.WriteTo(c.batchbuf); err != nil {
			return total, err
		}
	}
	return total, err
}

// ReadOffset sends a READ request, returning a scanner that can be used to
// iterate over the messages in the response.
func (c *Client) ReadOffset(topic []byte, offset uint64, limit int) (int, *protocol.BatchScanner, error) {
	internal.Debugf(c.gconf, "READ %s %d %d -> %s", topic, offset, limit, c.RemoteAddr())
	req := c.readreq
	req.Reset()
	req.SetTopic(topic)
	req.Offset = offset
	req.Messages = limit

	if _, _, err := c.doRequest(req); err != nil {
		return 0, nil, err
	}

	respOff, nbatches, err := c.readBatchResponse()
	if err != nil {
		return 0, nil, err
	}
	if respOff != offset {
		log.Printf("response offset (%d) did not match request (%d)", respOff, offset)
		return 0, nil, protocol.ErrInternal
	}

	if _, err := c.readBatches(nbatches, c.br); err != nil {
		return nbatches, nil, err
	}
	c.batchbr.Reset(c.batchbuf)
	c.bs.Reset(c.batchbr)
	internal.LogError(c.SetReadDeadline(time.Now().Add(c.readTimeout)))
	return nbatches, c.bs, nil
}

// Tail sends a TAIL request, returning the initial offset and a scanner
// starting from the first available batch.
func (c *Client) Tail(topic []byte, limit int) (uint64, int, *protocol.BatchScanner, error) {
	internal.Debugf(c.gconf, "TAIL %s %d -> %s", topic, limit, c.RemoteAddr())
	req := c.tailreq
	req.Reset()
	req.SetTopic(topic)
	req.Messages = limit

	if _, _, err := c.doRequest(req); err != nil {
		return 0, 0, nil, err
	}

	respOff, nbatches, err := c.readBatchResponse()
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
		if c.closer != nil {
			internal.LogError(c.closer.Close())
		}
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

// Config sends a CONFIG request, returning parts the server's configuration
// relevant to the client.
func (c *Client) Config() (*config.Config, error) {
	confreq := protocol.NewConfigRequest(c.gconf)
	if _, _, err := c.doRequest(confreq); err != nil {
		return nil, err
	}

	confResp := protocol.NewConfigResponse(c.gconf)
	if err := confResp.Parse(c.cr.MultiResp()); err != nil {
		return nil, err
	}

	return confResp.Config(), nil
}

func (c *Client) doRequest(wt io.WriterTo) (int64, int64, error) {
	sent, recv, err := c.do(wt)
	if err != nil {
		return c.retryRequest(wt, sent, recv, err)
	}
	return sent, recv, err
}

func (c *Client) do(wt io.WriterTo) (int64, int64, error) {
	if err := c.ensureConn(); err != nil {
		return 0, 0, err
	}

	internal.LogError(c.SetWriteDeadline(time.Now().Add(c.writeTimeout)))
	sent, err := wt.WriteTo(c.bw)
	internal.LogError(c.SetWriteDeadline(time.Time{}))
	if err != nil {
		return sent, 0, err
	}

	err = c.flush()
	internal.Debugf(c.gconf, "wrote %d bytes to %s (err: %v)", sent, c.RemoteAddr(), err)
	if err != nil {
		return sent, 0, err
	}

	recv, err := c.readClientResponse()
	return sent, recv, err
}

func (c *Client) ensureConn() error {
	if c.Conn != nil {
		return nil
	}

	err := c.connect(c.conf.Hostport)
	return err
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
	for c.conf.ConnRetries < 0 || c.retries < c.conf.ConnRetries {
		if retryErr != nil && !isRetryable(retryErr) {
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

		if c.closer != nil {
			internal.IgnoreError(c.conf.Verbose, c.closer.Close())
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
		return sent, recv, nil
	}

	if retryErr != nil {
		return sent, recv, retryErr
	}
	return sent, recv, err
}

// flush flushes all pending data to the server
func (c *Client) flush() error {
	if c.bw.Buffered() > 0 {
		internal.Debugf(c.gconf, "client.Flush() initiated (%d bytes)", c.bw.Buffered())
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
	internal.Debugf(c.gconf, "read %d bytes from %s: %+v (err: %v)", n, c.RemoteAddr(), c.cr, err)
	return n, c.handleErr(err)
}

func (c *Client) readBatchResponse() (uint64, int, error) {
	return c.cr.Offset(), c.cr.Batches(), c.cr.Error()
}

func (c *Client) readCloseResponse() error {
	if !c.cr.Ok() {
		return errors.New("close failed")
	}
	return nil
}

func (c *Client) readConfigResponse() error {
	b := c.cr.MultiResp()
	if len(b) < 1 {
		return errors.New("multi response empty")
	}

	return nil
}

func (c *Client) handleErr(err error) error {
	if err == nil {
		return err
	}
	if err == io.EOF {
		internal.DebugfDepth(c.gconf, 1, "%s closed the connection", c.RemoteAddr())
	} else {
		internal.DebugfDepth(c.gconf, 1, "%+v", err)
	}
	return err
}

func isRetryable(err error) bool {
	if err == nil || err == io.EOF || err == io.ErrClosedPipe {
		return true
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() || nerr.Temporary() {
		return true
	}
	return false
}
