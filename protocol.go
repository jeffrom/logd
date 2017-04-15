package logd

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
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

func (pw *protoWriter) writeCommand(cmd *command) (int, error) {
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

func (pw *protoWriter) writeResponse(resp *response) (int, error) {

	return 0, nil
}

// turns a []byte of socket data into a *response or *readResponse
type protoReader struct {
	config *Config
	br     *bufio.Reader
	r      io.Reader
}

func newProtoReader(r io.Reader, config *Config) *protoReader {
	return &protoReader{r: r, br: bufio.NewReader(r), config: config}
}

func (pr *protoReader) readCommand() (*command, error) {
	line, err := readLine(pr.br)
	if err != nil {
		return nil, err
	}
	debugf(pr.config, "read(raw): %q", line)

	parts := bytes.SplitN(line, []byte(" "), 2)
	if len(parts) != 2 {
		return nil, errors.New("Badly formatted command")
	}

	name := cmdNamefromBytes(parts[0])
	numArgs, err := strconv.ParseInt(string(parts[1]), 10, 16)
	if err != nil {
		return nil, err
	}

	var args [][]byte
	// TODO read args efficiently
	for i := 0; i < int(numArgs); i++ {
		line, err = readLine(pr.br)
		if err != nil {
			return nil, err
		}
		debugf(pr.config, "read arg(raw): %q", line)

		parts = bytes.SplitN(line, []byte(" "), 2)
		if len(parts) != 2 {
			return nil, errors.New("Badly formatted argument")
		}

		_, err := strconv.ParseUint(string(parts[0]), 10, 64)
		if err != nil {
			return nil, errors.New("Badly formatted argument length")
		}

		arg := parts[1]

		args = append(args, arg)
	}

	return newCommand(name, args...), nil
}

func (pr *protoReader) readResponse() (*response, error) {
	line, err := readLine(pr.br)
	if err != nil {
		return nil, err
	}

	parts := bytes.SplitN(line, []byte(" "), 2)
	var resp *response
	if bytes.Equal(parts[0], []byte("OK")) {
		resp = newResponse(respOK)
	} else if bytes.Equal(parts[0], []byte("+EOF")) {
		resp = newResponse(respEOF)
		// } else if parts[0][0] == '+' {
		// 	resp = newResponse(respContinue)
	} else if bytes.Equal(parts[0], []byte("ERR")) {
		resp = newErrResponse(parts[1])
	} else if bytes.Equal(parts[0], []byte("ERR_CLIENT")) {
		resp = newClientErrResponse(parts[1])
	} else {
		debugf(pr.config, "invalid response: %q", line)
		return nil, errors.New("Invalid response")
	}

	return resp, nil
}

type protoScanner struct {
	config *Config
	br     *bufio.Reader
	conn   net.Conn
	err    error
	msg    *message
}

func newProtoScanner(r io.Reader, conn net.Conn, config *Config) *protoScanner {
	return &protoScanner{br: bufio.NewReader(r), conn: conn, config: config}
}

func (ps *protoScanner) Scan() bool {
	err := ps.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if err != nil {
		ps.err = err
		return false
	}

	msg, err := ps.readMessage()
	if err != nil {
		ps.err = err
		return false
	}

	ps.err = nil
	ps.msg = msg
	return true
}

func (ps *protoScanner) Message() *message {
	return ps.msg
}

func (ps *protoScanner) Err() error {
	return ps.err
}

func (ps *protoScanner) readMessage() (*message, error) {
	var id uint64
	var body []byte
	var bodylen int
	var err error

	line, err := readLine(ps.br)
	if err != nil {
		ps.err = err
		return nil, err
	}

	if line[0] != '+' {
		return nil, errors.New("invalid first byte")
	}

	parts := bytes.SplitN(line, []byte(" "), 3)
	if len(parts) != 3 {
		return nil, errors.New("invalid protocol line")
	}

	_, err = fmt.Sscanf(string(parts[0]), "+%d", &id)
	if err != nil {
		fmt.Println("fuck", err)
		return nil, err
	}

	_, err = fmt.Sscanf(string(parts[1]), "%d", &bodylen)
	if err != nil {
		return nil, err
	}

	body = parts[2]
	if bodylen != len(body) {
		return nil, errors.New("invalid body length")
	}

	return newMessage(id, body), err
}
