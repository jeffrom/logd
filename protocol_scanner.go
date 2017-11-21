package logd

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

// text command protocol works as follows:
// <cmd> <num_args>\r\n
// <arglen> <arg>\r\n
// ...

// normal responses look like:
//
// OK <id> <body>\r\n
// ERR <reason>\r\n
// ERR_CLIENT <reason>\r\n
// EOF\r\n

// read protocol is chunked as follows:
//
// +<length>\r\n
// body\r\n
//
// if the server closes the connection, and EOF is sent:
//
// +EOF\r\n
//
// body consists of messages, each with an incrementing ID. The same format is
// used for file storage:
//
// <id> <length> <crc> <body>\r\n
//

var errInvalidFirstByte = errors.New("invalid first byte")
var errReadStopped = errors.New("read stopped by other side")
var errInvalidProtocolLine = errors.New("invalid protocol line")
var errInvalidBodyLength = errors.New("invalid body length")
var errCrcChecksumMismatch = errors.New("crc checksum mismatch")

// ProtocolScanner reads the log protocol
type ProtocolScanner struct {
	config       *Config
	br           *bufio.Reader
	lastChunkPos int64
	chunkPos     int64
	chunkEnd     int64
	msg          *Message
	err          error
}

func newProtocolScanner(config *Config, r io.Reader) *ProtocolScanner {
	return &ProtocolScanner{
		config: config,
		br:     bufio.NewReaderSize(r, config.PartitionSize),
	}
}

// Scan reads over log data in a loop
func (ps *ProtocolScanner) Scan() bool {
	if ps.chunkEnd <= 0 { // need to read chunk envelope
		if err := ps.scanEnvelope(); err != nil && err != errInvalidFirstByte {
			ps.err = err
			return false
		}
	}

	n, msg, err := ps.readMessage()
	ps.lastChunkPos = int64(n)
	ps.chunkPos += int64(n)
	if ps.chunkPos >= ps.chunkEnd {
		ps.chunkEnd = 0
	}
	ps.err = err

	ps.msg = msg
	return err == nil
}

func (ps *ProtocolScanner) readMessage() (int, *Message, error) {
	var id uint64
	var body []byte
	var bodylen int64
	var checksum uint64
	var err error
	var read int

	line, err := readLine(ps.br)
	fmt.Printf("read: %q (%v)\n", line, err)
	read += len(line)
	if err != nil {
		ps.err = err
		return read, nil, err
	}
	read += 2 // \r\n

	parts := bytes.SplitN(line, []byte(" "), 4)
	if len(parts) != 4 {
		// if len(parts) == 1 && bytes.Equal(parts[0], []byte("+EOF")) {
		// 	return nil, io.EOF
		// }
		return read, nil, errInvalidProtocolLine
	}

	if id, err = strconv.ParseUint(string(parts[0]), 10, 64); err != nil {
		return read, nil, errors.Wrap(err, "scanning id failed")
	}

	if bodylen, err = strconv.ParseInt(string(parts[1]), 10, 64); err != nil {
		return read, nil, errors.Wrap(err, "scanning body length failed")
	}

	if checksum, err = strconv.ParseUint(string(parts[2]), 10, 32); err != nil {
		return read, nil, errors.Wrap(err, "failed to scan crc")
	}

	body = parts[3]
	if int(bodylen) != len(body) {
		return read, nil, errInvalidBodyLength
	}

	if crc32.Checksum(body, crcTable) != uint32(checksum) {
		return read, nil, errCrcChecksumMismatch
	}

	return read, NewMessage(id, bytes.TrimRight(body, "\r\n")), err
}

func (ps *ProtocolScanner) scanEnvelope() error {
	if b, err := ps.br.Peek(1); err != nil {
		if err == io.EOF {
			return nil
		}
		return errors.Wrap(err, "failed reading first byte")
	} else if b[0] != '+' {
		return errInvalidFirstByte
	}
	ps.br.ReadByte()

	line, err := readLine(ps.br)
	if err != nil {
		return err
	}

	if bytes.Equal(line, []byte("EOF")) {
		return io.EOF
	}

	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return errors.Wrap(err, "failed to parse chunk length")
	}
	ps.chunkEnd = n

	return nil
}

// Message returns the message of the current iteration
func (ps *ProtocolScanner) Message() *Message {
	return ps.msg
}

func (ps *ProtocolScanner) Error() error {
	return ps.err
}

// protocolWriter constructs all command request and response bytes.
type protocolWriter struct {
	buf bytes.Buffer
}

func newProtocolWriter() *protocolWriter {
	return &protocolWriter{}
}

func (pw *protocolWriter) writeCommand(cmd *Command) []byte {
	buf := pw.buf
	buf.Reset()
	buf.WriteString(cmd.name.String())
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatInt(int64(len(cmd.args)), 10))
	buf.WriteString("\r\n")
	// buf.WriteString(fmt.Sprintf("%s %d\r\n", cmd.name.String(), len(cmd.args)))
	for _, arg := range cmd.args {
		buf.WriteString(strconv.FormatInt(int64(len(arg)), 10))
		buf.WriteByte(' ')
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	return buf.Bytes()
}

func (pw *protocolWriter) writeChunkEnvelope(b []byte) []byte {
	buf := pw.buf
	buf.Reset()
	buf.WriteByte('+')
	buf.WriteString(strconv.FormatInt(int64(len(b)), 10))
	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (pw *protocolWriter) writeResponse(r *Response) []byte {
	if r.Status == RespEOF {
		return []byte("+EOF\r\n")
	}

	buf := pw.buf
	buf.Reset()
	buf.WriteString(r.Status.String())

	if r.ID > 0 && r.body != nil {
		panic("response id and body both set")
	}

	if r.ID != 0 {
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatUint(r.ID, 10))
	}
	if r.body != nil {
		buf.WriteByte(' ')
		buf.Write(r.body)
	}

	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (pw *protocolWriter) writeLogLine(m *Message) []byte {
	checksum := crc32.Checksum(m.Body, crcTable)
	return []byte(fmt.Sprintf("%d %d %d %s\r\n", m.ID, len(m.Body), checksum, m.Body))
}

// protocolReader reads commands and responses
// turns a []byte of socket data into a *response or *readResponse

type protocolReader struct {
	config *Config
	br     *bufio.Reader
}

func newProtocolReader(config *Config) *protocolReader {
	return &protocolReader{
		config: config,
		br:     bufio.NewReaderSize(bytes.NewReader([]byte("")), config.PartitionSize),
	}
}

func (pr *protocolReader) readCommand(r io.Reader) (*Command, error) {
	pr.br.Reset(r)

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
		debugf(pr.config, "read arg(raw): %q", prettybuf(line))

		parts = bytes.SplitN(line, []byte(" "), 2)
		if len(parts) != 2 {
			return nil, errors.New("Badly formatted argument")
		}

		_, err := parseNumber(parts[0])
		if err != nil {
			return nil, errors.New("Badly formatted argument length")
		}

		arg := parts[1]

		args = append(args, arg)
	}

	return NewCommand(name, args...), nil
}

func (pr *protocolReader) readResponse(r io.Reader) (*Response, error) {
	pr.br.Reset(r)
	line, err := readLine(pr.br)
	if err != nil {
		return nil, err
	}

	parts := bytes.SplitN(line, []byte(" "), 2)
	var resp *Response
	if bytes.Equal(parts[0], []byte("OK")) {
		resp = newResponse(RespOK)
		if len(parts) > 1 {
			if _, err := fmt.Sscanf(string(parts[1]), "%d", &resp.ID); err != nil {
				return nil, errors.Wrap(err, "failed to parse response id")
			}
		}
	} else if bytes.Equal(parts[0], []byte("+EOF")) {
		resp = newResponse(RespEOF)
		// } else if parts[0][0] == '+' {
		// 	resp = newResponse(RespContinue)
	} else if bytes.Equal(parts[0], []byte("ERR")) {
		var arg []byte
		if len(parts) > 1 {
			arg = parts[1]
		}
		resp = NewErrResponse(arg)
	} else if bytes.Equal(parts[0], []byte("ERR_CLIENT")) {
		resp = NewClientErrResponse(parts[1])
	} else {
		debugf(pr.config, "invalid response: %q", line)
		return nil, errors.New("Invalid response")
	}

	return resp, nil
}
