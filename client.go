package logd

// NOTE CLIENT

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// Client represents a connection to the database
type Client struct {
	conn   net.Conn
	config *Config

	readTimeout time.Duration
	pr          *protocolReader

	pw           *protocolWriter
	writeTimeout time.Duration
}

// Dial returns a new instance of Conn
func Dial(addr string, conns ...net.Conn) (*Client, error) {
	return DialConfig(addr, DefaultConfig, conns...)
}

// DialConfig returns a configured Conn
func DialConfig(addr string, config *Config, conns ...net.Conn) (*Client, error) {
	debugf(config, "starting options: %s", config)
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

	timeout := time.Duration(config.ClientTimeout) * time.Millisecond
	return &Client{
		config:       config,
		conn:         conn,
		pr:           newProtocolReader(config),
		readTimeout:  timeout,
		pw:           newProtocolWriter(),
		writeTimeout: timeout,
	}, nil

}

func (c *Client) writeCommand(cmd *Command) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	if _, err := c.conn.Write(c.pw.writeCommand(cmd)); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return err
	}
	debugf(c.config, "%s->%s: %q", c.conn.LocalAddr(), c.conn.RemoteAddr(), cmd.Bytes())
	return nil
}

func (c *Client) flush() error {
	return nil
	// return c.pw.bw.Flush()
}

func (c *Client) readResponse() (*Response, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return nil, err
	}
	resp, err := c.pr.readResponse(c.conn)
	if err != nil {
		return nil, err
	}

	debugf(c.config, "%s<-%s: %q", c.conn.LocalAddr(), c.conn.RemoteAddr(), resp.Bytes())
	return resp, nil
}

func (c *Client) readScanResponse() (*ProtocolScanner, error) {
	resp, err := c.pr.readResponse(c.conn)
	if c.handleErr(err) != nil {
		return nil, err
	}

	debugf(c.config, "initial scan response: %s", resp.Status)

	if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return nil, err
	}
	return newProtocolScanner(c.config, c.pr.br), nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	debugf(c.config, "closing %s->%s", c.conn.LocalAddr(), c.conn.RemoteAddr())

	err := c.writeCommand(NewCommand(c.config, CmdClose))
	if c.handleErr(err) != nil {
		log.Printf("close error: %s", err)
		c.conn.Close()
		return err
	}

	_, err = c.readResponse()
	if c.handleErr(err) != nil {
		log.Printf("close error: %s", err)
		c.conn.Close()
		return err
	}

	debugf(c.config, "closing conn")
	return c.handleErr(c.conn.Close())
}

func (c *Client) handleErr(err error) error {
	if err == nil {
		return err
	}
	if err == io.EOF {
		debugf(c.config, "%s closed the connection", c.conn.RemoteAddr())
	}
	if err, ok := err.(net.Error); ok && err.Timeout() {
		debugf(c.config, "%s timed out", c.conn.RemoteAddr())
	}
	return err
}

// Do executes a command and returns the response.
func (c *Client) Do(cmds ...*Command) (*Response, error) {
	for _, cmd := range cmds {
		if err := c.writeCommand(cmd); c.handleErr(err) != nil {

			return nil, err
		}
	}
	if err := c.flush(); c.handleErr(err) != nil {
		return nil, err
	}
	return c.readResponse()
}

// DoRead returns a scanner that can be used to loop over messages, similar to
// bufio.Scanner
func (c *Client) DoRead(id uint64, limit int) (*ProtocolScanner, error) {
	debugf(c.config, "DoRead(%d, %d)", id, limit)
	cmd := NewCommand(c.config, CmdRead,
		[]byte(fmt.Sprintf("%d", id)),
		[]byte(fmt.Sprintf("%d", limit)),
	)
	if err := c.writeCommand(cmd); c.handleErr(err) != nil {
		return nil, err
	}

	deadline := time.Time{}
	if limit > 0 {
		deadline = time.Now().Add(c.readTimeout)
	}
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}

	return c.readScanResponse()
}

func (c *Client) Write(p []byte) (int, error) {
	if err := c.writeCommand(NewCommand(c.config, CmdMessage, p)); c.handleErr(err) != nil {
		return 0, err
	}
	return len(p), nil
}

// SetDeadline sets the timeout for the next io operation.
func (c *Client) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("%s (possible server error)", string(pe))
}

func (c *Client) readLine() ([]byte, error) {
	return readLine(c.pr.br)
}

func (c *Client) resetLimit() {
}
