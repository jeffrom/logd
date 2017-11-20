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

type protocolScanner struct {
	config   *Config
	br       *bufio.Reader
	chunkpos int64
	chunkend int64
	msg      *Message
	err      error
}

func newProtocolScanner(config *Config, r io.Reader) *protocolScanner {
	return &protocolScanner{
		config: config,
		br:     bufio.NewReaderSize(r, config.PartitionSize),
	}
}

func (ps *protocolScanner) Scan() bool {
	if ps.chunkend <= 0 { // need to read chunk envelope
		if err := ps.scanEnvelope(); err != nil {
			ps.err = err
			return false
		}
	}

	msg, err := ps.readMessage()

	ps.msg = msg
	if err != nil {
		ps.err = err
		return false
	}

	ps.err = nil
	return true
}

func (ps *protocolScanner) readMessage() (*Message, error) {
	var id uint64
	var body []byte
	var bodylen int64
	var checksum uint64
	var err error

	line, err := readLine(ps.br)
	if err != nil {
		ps.err = err
		return nil, err
	}

	parts := bytes.SplitN(line, []byte(" "), 4)
	if len(parts) != 4 {
		if len(parts) == 1 && bytes.Equal(parts[0], []byte("+EOF")) {
			return nil, io.EOF
		}
		return nil, errInvalidProtocolLine
	}

	if id, err = strconv.ParseUint(string(parts[0]), 10, 64); err != nil {
		return nil, errors.Wrap(err, "scanning id failed")
	}

	if bodylen, err = strconv.ParseInt(string(parts[1]), 10, 64); err != nil {
		return nil, errors.Wrap(err, "scanning body length failed")
	}

	if checksum, err = strconv.ParseUint(string(parts[2]), 10, 32); err != nil {
		return nil, errors.Wrap(err, "failed to scan crc")
	}

	body = parts[3]
	if int(bodylen) != len(body) {
		return nil, errInvalidBodyLength
	}

	if crc32.Checksum(body, crcTable) != uint32(checksum) {
		return nil, errCrcChecksumMismatch
	}

	return NewMessage(id, body), err
}

func (ps *protocolScanner) scanEnvelope() error {
	if ch, err := ps.br.ReadByte(); err != nil {
		return errors.Wrap(err, "failed reading first byte")
	} else if ch != '+' {
		return errInvalidFirstByte
	}

	line, err := readLine(ps.br)
	if err != nil {
		return err
	}

	if bytes.Equal(line, []byte("EOF")) {
		return errReadStopped
	}

	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return errors.Wrap(err, "failed to parse chunk length")
	}
	ps.chunkend = n

	return nil
}

func (ps *protocolScanner) Message() *Message {
	return ps.msg
}

func (ps *protocolScanner) Error() error {
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
	buf.WriteString(strconv.FormatInt(int64(len(b)), 10))
	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (pw *protocolWriter) writeResponse(r *Response) []byte {
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

	// TODO add crc

	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (pw *protocolWriter) writeLogLine(m *Message) []byte {
	checksum := crc32.Checksum(m.Body, crcTable)
	return []byte(fmt.Sprintf("%d %d %d %s\r\n", m.ID, len(m.Body), checksum, m.Body))
}
