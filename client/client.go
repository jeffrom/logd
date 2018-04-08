package client

// NOTE CLIENT

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

var errNotFound = errors.New("id not found")

// Client represents a connection to the database
type Client struct {
	net.Conn
	config *config.Config

	readTimeout time.Duration
	pr          *protocol.ProtocolReader

	bw           *bufio.Writer
	pw           *protocol.ProtocolWriter
	writeTimeout time.Duration
}

// Dial returns a new instance of Conn
func Dial(addr string, conns ...net.Conn) (*Client, error) {
	return DialConfig(addr, config.DefaultConfig, conns...)
}

// DialConfig returns a configured Conn
func DialConfig(addr string, conf *config.Config, conns ...net.Conn) (*Client, error) {
	internal.Debugf(conf, "starting options: %s", conf)
	var conn net.Conn
	var err error

	if len(conns) == 1 {
		conn = conns[0]
	} else {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			if conn != nil {
				conn.Close()
			}

			// if nerr, ok := err.(*net.OpError); ok {
			// 	if nerr.Op == "dial" {
			// 		// unknown host
			// 	} else if nerr.Op == "read" {
			// 		// connection refused
			// 	}
			// }

			return nil, err
		}
	}

	timeout := time.Duration(conf.ClientTimeout) * time.Millisecond
	return &Client{
		config:       conf,
		Conn:         conn,
		pr:           protocol.NewProtocolReader(conf),
		readTimeout:  timeout,
		bw:           bufio.NewWriter(conn),
		pw:           protocol.NewProtocolWriter(),
		writeTimeout: timeout,
	}, nil
}

// Do executes a command and returns the response.
func (c *Client) Do(cmds ...*protocol.Command) (*protocol.Response, error) {
	for _, cmd := range cmds {
		if err := c.WriteCommand(cmd); c.handleErr(err) != nil {

			return nil, err
		}
	}
	if err := c.Flush(); c.handleErr(err) != nil {
		return nil, err
	}
	return c.readResponse()
}

// DoRead returns a scanner that can be used to loop over messages, similar to
// bufio.Scanner
func (c *Client) DoRead(id uint64, limit int) (*Scanner, error) {
	internal.Debugf(c.config, "DoRead(%d, %d)", id, limit)
	cmdType := protocol.CmdRead
	if c.config.ReadFromTail {
		cmdType = protocol.CmdTail
	}

	cmd := protocol.NewCommand(c.config, cmdType,
		[]byte(fmt.Sprintf("%d", id)),
		[]byte(fmt.Sprintf("%d", limit)),
	)
	timeout := time.Duration(c.config.ClientTimeout) * time.Millisecond
	if err := c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := c.WriteCommand(cmd); c.handleErr(err) != nil {
		return nil, err
	}
	if err := c.Flush(); c.handleErr(err) != nil {
		return nil, err
	}

	return newScanner(c.config, c).fromScanResponse()
}

func (c *Client) continueRead(id uint64, limit int) (*protocol.Scanner, error) {
	internal.Debugf(c.config, "continueRead(%d, %d)", id, limit)
	cmd := protocol.NewCommand(c.config, protocol.CmdRead,
		[]byte(fmt.Sprintf("%d", id)),
		[]byte(fmt.Sprintf("%d", limit)))

	timeout := time.Duration(c.config.ClientTimeout) * time.Millisecond
	if err := c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := c.WriteCommand(cmd); c.handleErr(err) != nil {
		return nil, err
	}
	if err := c.Flush(); c.handleErr(err) != nil {
		return nil, err
	}

	return c.readScanResponse(true)
}

func (c *Client) Write(p []byte) (int, error) {
	if err := c.WriteCommand(protocol.NewCommand(c.config, protocol.CmdMessage, p)); c.handleErr(err) != nil {
		return 0, err
	}
	return len(p), nil
}

// SetDeadline sets the timeout for the next io operation.
func (c *Client) SetDeadline(t time.Time) error {
	return c.Conn.SetDeadline(t)
}

// Close closes the client connection.
func (c *Client) Close() error {
	internal.Debugf(c.config, "closing %s->%s", c.Conn.LocalAddr(), c.Conn.RemoteAddr())

	err := c.WriteCommand(protocol.NewCommand(c.config, protocol.CmdClose))
	if c.handleErr(err) != nil {
		log.Printf("%s: close error: %+v", c.Conn.RemoteAddr(), err)
		c.Conn.Close()
		return err
	}

	_, err = c.readResponse()
	if c.handleErr(err) != nil {
		log.Printf("%s: close error: %+v", c.Conn.RemoteAddr(), err)
		c.Conn.Close()
		return err
	}

	internal.Debugf(c.config, "closing conn")
	return c.handleErr(c.Conn.Close())
}

// WriteCommand issues a protocol command to the server
// TODO make this private
func (c *Client) WriteCommand(cmd *protocol.Command) error {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	if _, err := cmd.WriteTo(c.bw); err != nil {
		return err
	}
	if err := c.Flush(); err != nil {
		return err
	}
	// internal.Debugf(c.config, "%s->%s: %q", c.Conn.LocalAddr(), c.Conn.RemoteAddr(), cmd.Bytes())
	return nil
}

// Flush flushes all pending data to the server
func (c *Client) Flush() error {
	// return nil
	return c.bw.Flush()
}

func (c *Client) readResponse() (*protocol.Response, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return nil, err
	}
	resp, err := c.pr.ReadResponse(c.Conn)
	if err != nil {
		return nil, err
	}

	b, _ := resp.SprintBytes()
	internal.Debugf(c.config, "%s<-%s: %q", c.Conn.LocalAddr(), c.Conn.RemoteAddr(), b)
	return resp, nil
}

func (c *Client) readScanResponse(forever bool) (*protocol.Scanner, error) {
	deadline := time.Now().Add(c.readTimeout)

	if err := c.Conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}

	resp, err := c.pr.ReadResponse(c.Conn)
	if c.handleErr(err) != nil {
		return nil, err
	}

	if resp.Failed() {
		if bytes.Equal(resp.Body, protocol.ErrRespNotFound) {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("%s %s", resp.Status, resp.Body)
	}

	internal.Debugf(c.config, "initial scan response: %s", resp.Status)
	return protocol.NewScanner(c.config, c.pr.Br), nil
}

func (c *Client) handleErr(err error) error {
	if err == nil {
		return err
	}
	if err == io.EOF {
		internal.Debugf(c.config, "%s closed the connection", c.Conn.RemoteAddr())
	}
	if err, ok := err.(net.Error); ok && err.Timeout() {
		internal.Debugf(c.config, "%s timed out", c.Conn.RemoteAddr())
	}
	return err
}

func (c *Client) readLine() ([]byte, error) {
	return protocol.ReadLine(c.pr.Br)
}

func (c *Client) resetLimit() {
}
