package logd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// text protocol works as follows:
// <cmd> <num_args>\r\n
// <arglen> <arg>\r\n
// ...

// read works like:
// request:  READ(<id>, <limit>)
// response: OK\r\n
//           +<id> <len> <body>\r\n
//           ...
//					 +EOF\r\n

type protoWriter struct {
	config *Config
	w      io.Writer
	bw     *bufio.Writer
}

func newProtoWriter(w io.Writer, config *Config) *protoWriter {
	return &protoWriter{w: w, bw: bufio.NewWriter(w), config: config}
}

func (pw *protoWriter) writeCommand(cmd *Command) (int, error) {
	buf := pw.bw
	buf.WriteString(fmt.Sprintf("%s %d\r\n", cmd.name.String(), len(cmd.args)))
	for _, arg := range cmd.args {
		buf.WriteString(strconv.FormatInt(int64(len(arg)), 10))
		buf.WriteByte(' ')
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	return 0, nil
}

func (pw *protoWriter) writeResponse(resp *Response) (int, error) {

	return 0, nil
}

// Scanner is used to loop over the result of a READ command
type Scanner struct {
	config *Config
	br     *bufio.Reader
	conn   net.Conn
	err    error
	msg    *Message
}

func newScanner(r io.Reader, conn net.Conn, config *Config) *Scanner {
	return &Scanner{br: bufio.NewReader(r), conn: conn, config: config}
}

// Scan reads the next result from a READ command.
func (ps *Scanner) Scan() bool {
	ps.conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
	msg, err := ps.readMessage()
	ps.msg = msg
	if err != nil {
		ps.err = err
		return false
	}

	ps.err = nil
	return true
}

// Message returns the current message. It will be overwritten on the next
// iteration.
func (ps *Scanner) Message() *Message {
	return ps.msg
}

// Err returns the current error.
func (ps *Scanner) Err() error {
	return ps.err
}

func (ps *Scanner) readMessage() (*Message, error) {
	var id uint64
	var body []byte
	var bodylen int
	var err error

	line, err := readLine(ps.br)
	if err != nil {
		ps.err = err
		return nil, err
	}

	// fmt.Printf("%q\n", line)
	if line[0] != '+' {
		return nil, errors.New("invalid first byte")
	}

	parts := bytes.SplitN(line, []byte(" "), 3)
	if len(parts) != 3 {
		if len(parts) == 1 && bytes.Equal(parts[0], []byte("+EOF")) {
			return nil, io.EOF
		}
		return nil, errors.New("invalid protocol line")
	}

	_, err = fmt.Sscanf(string(parts[0]), "+%d", &id)
	if err != nil {
		return nil, errors.Wrap(err, "scanning id failed")
	}

	_, err = fmt.Sscanf(string(parts[1]), "%d", &bodylen)
	if err != nil {
		return nil, errors.Wrap(err, "scanning body length failed")
	}

	body = parts[2]
	if bodylen != len(body) {
		return nil, errors.New("invalid body length")
	}

	return NewMessage(id, body), err
}

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("long response line")
	}
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrap(err, "reading line failed")
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
