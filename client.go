package logd

// NOTE CLIENT

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

// Client represents a connection to the database
type Client struct {
	conn   net.Conn
	config *Config

	readTimeout time.Duration
	pr          *protoReader

	pw           *protoWriter
	writeTimeout time.Duration
}

// Dial returns a new instance of Conn
func Dial(addr string, conns ...net.Conn) (*Client, error) {
	return dialConfig(addr, defaultConfig, conns...)
}

func dialConfig(addr string, config *Config, conns ...net.Conn) (*Client, error) {
	var conn net.Conn
	var err error

	if len(conns) == 1 {
		conn = conns[0]
	} else {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	return &Client{
		config:       config,
		conn:         conn,
		pr:           newProtoReader(conn, config),
		readTimeout:  time.Duration(500 * time.Millisecond),
		pw:           newProtoWriter(conn, config),
		writeTimeout: time.Duration(500 * time.Millisecond),
	}, nil

}

func (c *Client) writeCommand(cmd *command) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	if _, err := c.pw.writeCommand(cmd); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return err
	}
	debugf(c.config, "%s->%s: %q", c.conn.LocalAddr(), c.conn.RemoteAddr(), cmd.Bytes())
	return nil
}

func (c *Client) flush() error {
	return c.pw.bw.Flush()
}

func (c *Client) readResponse() (*response, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return nil, err
	}
	resp, err := c.pr.readResponse()
	if err != nil {
		return nil, err
	}

	debugf(c.config, "%s<-%s: %q", c.conn.LocalAddr(), c.conn.RemoteAddr(), resp.Bytes())
	return resp, nil
}

func (c *Client) readScanResponse() (*protoScanner, error) {
	resp, err := c.pr.readResponse()
	if err != nil {
		return nil, err
	}

	debugf(c.config, "initial scan response: %s", resp.status)

	return newProtoScanner(c.pr.br, c.conn, c.config), nil
}

func (c *Client) close() error {
	debugf(c.config, "closing %s->%s", c.conn.LocalAddr(), c.conn.RemoteAddr())

	err := c.writeCommand(newCommand(cmdClose))
	if err != nil {
		log.Printf("close error: %s", err)
		c.conn.Close()
		return err
	}

	_, err = c.readResponse()
	if err != nil {
		log.Printf("close error: %s", err)
		c.conn.Close()
		return err
	}

	debugf(c.config, "closing conn")
	return c.conn.Close()
}

func (c *Client) do(cmds ...*command) (*response, error) {
	for _, cmd := range cmds {
		if err := c.writeCommand(cmd); err != nil {
			return nil, err
		}
	}
	if err := c.flush(); err != nil {
		return nil, err
	}

	return c.readResponse()
}

// returns a scanner that can be used to loop over messages, similar to
// bufio.Scanner
func (c *Client) doRead(id uint64, limit int) (*protoScanner, error) {
	cmd := newCommand(cmdRead,
		[]byte(fmt.Sprintf("%d", id)),
		[]byte(fmt.Sprintf("%d", limit)),
	)
	if err := c.writeCommand(cmd); err != nil {
		return nil, err
	}
	return c.readScanResponse()
}

func (c *Client) Write(p []byte) (int, error) {
	if err := c.writeCommand(newCommand(cmdMsg, p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("%s (possible server error)", string(pe))
}

func (c *Client) readLine() ([]byte, error) {
	return readLine(c.pr.br)
}

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("long response line")
	}
	if err != nil {
		return nil, err
	}

	if len(line) < termLen {
		return nil, protocolError("line missing terminator")
	}

	if line[len(line)-1] != '\n' || line[len(line)-2] != '\r' {
		return nil, protocolError("bad response line terminator")
	}

	line = line[:len(line)-2]
	return line, nil
}

func (c *Client) resetLimit() {
}
