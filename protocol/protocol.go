package protocol

// text command protocol works as follows:
// <cmd> <num_args>\r\n
// <arglen> <arg>\r\n
// ...

// normal responses look like:
//
// OK <id>\r\n
// OK <len> <body>\r\n
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

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
)

const termLen = 2

var errInvalidFirstByte = errors.New("invalid first byte")
var errReadStopped = errors.New("read stopped by other side")
var errInvalidProtocolLine = errors.New("invalid protocol line")
var errInvalidBodyLength = errors.New("invalid body length")
var errCrcChecksumMismatch = errors.New("crc checksum mismatch")
var errRangeNotFound = errors.New("id range not found")

var crcTable = crc32.MakeTable(crc32.Koopman)

type protocolFlusher interface {
	shouldFlush() bool
}

type flushReader struct {
	r io.Reader
}

func (fr *flushReader) Read(p []byte) (int, error) {
	return fr.r.Read(p)
}

func (fr *flushReader) shouldFlush() bool {
	return true
}

func newFlushReader(r io.Reader) *flushReader {
	return &flushReader{r: r}
}

// ProtocolScanner reads the log protocol. The same protocol is used for both
// the file log and network chunk protocol.
type ProtocolScanner struct {
	config       *config.Config
	Br           *bufio.Reader
	LastChunkPos int64
	ChunkPos     int64
	chunkEnd     int64
	msg          *Message
	err          error
}

// NewProtocolScanner returns a new instance of a buffered protocol scanner.
func NewProtocolScanner(conf *config.Config, r io.Reader) *ProtocolScanner {
	// TODO maybe pass through bufio.Reader instead of creating a new one if r
	// is a bufio.Reader?
	return &ProtocolScanner{
		config: conf,
		Br:     bufio.NewReaderSize(r, 1024*8),
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

	n, msg, err := ps.ReadMessage()
	ps.LastChunkPos = int64(n)
	ps.ChunkPos += int64(n)
	if ps.chunkEnd > 0 && ps.ChunkPos >= ps.chunkEnd {
		internal.Debugf(ps.config, "completed reading %d byte chunk", ps.ChunkPos)
		ps.ChunkPos = 0
		ps.chunkEnd = 0
	}
	ps.err = err

	ps.msg = msg
	return err == nil
}

func (ps *ProtocolScanner) ReadMessage() (int, *Message, error) {
	var id uint64
	var body []byte
	var bodylen int64
	var checksum uint64
	var err error
	var read int

	// fmt.Println("reading line")
	line, err := ReadLine(ps.Br)
	// fmt.Printf("read: %q (length: %d) (err: %v)\n", line, len(line)+2, err)
	read += len(line)
	read += 2 // \r\n
	if err != nil {
		ps.err = err
		return read, nil, err
	}

	if bytes.Equal(line, []byte("+EOF")) {
		return read, nil, io.EOF
	}

	parts := bytes.SplitN(line, []byte(" "), 4)
	if len(parts) != 4 {
		// fmt.Printf("%q\n", parts)
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
	// fmt.Printf("%q\n", body)
	if int(bodylen) != len(body) {
		return read, nil, errInvalidBodyLength
	}

	if crc32.Checksum(body, crcTable) != uint32(checksum) {
		return read, nil, errCrcChecksumMismatch
	}

	return read, NewMessage(id, bytes.TrimRight(body, "\r\n")), err
}

func (ps *ProtocolScanner) scanEnvelope() error {
	if b, err := ps.Br.Peek(1); err != nil {
		if err == io.EOF {
			return err
		}
		return errors.Wrap(err, "failed reading first byte")
	} else if b[0] != '+' {
		return errInvalidFirstByte
	}
	ps.Br.ReadByte()
	// internal.Debugf(ps.config, "scanning envelope")

	line, err := ReadLine(ps.Br)
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

	internal.Debugf(ps.config, "scanned chunk envelope for %d bytes", n)
	return nil
}

// Message returns the message of the current iteration
func (ps *ProtocolScanner) Message() *Message {
	return ps.msg
}

func (ps *ProtocolScanner) Error() error {
	return ps.err
}

// ProtocolWriter constructs all command request and response bytes.
type ProtocolWriter struct {
	buf bytes.Buffer
}

// NewProtocolWriter returns a new instance of a protocol writer
func NewProtocolWriter() *ProtocolWriter {
	return &ProtocolWriter{}
}

func (pw *ProtocolWriter) WriteCommand(cmd *Command) []byte {
	buf := pw.buf
	buf.Reset()
	buf.WriteString(cmd.Name.String())
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatInt(int64(len(cmd.Args)), 10))
	buf.WriteString("\r\n")
	// buf.WriteString(fmt.Sprintf("%s %d\r\n", cmd.Name.String(), len(cmd.Args)))
	for _, arg := range cmd.Args {
		buf.WriteString(strconv.FormatInt(int64(len(arg)), 10))
		buf.WriteByte(' ')
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	return buf.Bytes()
}

func (pw *ProtocolWriter) writeChunkEnvelope(b []byte) []byte {
	buf := pw.buf
	buf.Reset()
	buf.WriteByte('+')
	buf.WriteString(strconv.FormatInt(int64(len(b)), 10))
	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (pw *ProtocolWriter) writeResponse(r *Response) []byte {
	if r.Status == RespEOF {
		return []byte("+EOF\r\n")
	}

	buf := pw.buf
	buf.Reset()
	buf.WriteString(r.Status.String())

	if r.ID > 0 && r.Body != nil {
		panic("response id and body both set")
	}

	if r.ID != 0 {
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatUint(r.ID, 10))
	} else if r.Status == RespOK && r.Body != nil {
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatInt(int64(len(r.Body)), 10))
		buf.WriteByte(' ')
		buf.Write(r.Body)
	} else if r.Body != nil {
		buf.WriteByte(' ')
		buf.Write(r.Body)
	}

	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (pw *ProtocolWriter) WriteLogLine(m *Message) []byte {
	checksum := crc32.Checksum(m.Body, crcTable)
	// fmt.Printf("write log: %d %d %d %q\n", m.ID, len(m.Body), checksum, m.Body)
	return []byte(fmt.Sprintf("%d %d %d %s\r\n", m.ID, len(m.Body), checksum, m.Body))
}

// ProtocolReader reads commands and responses
// turns a []byte of socket data into a *response or *ReadResponse
type ProtocolReader struct {
	config *config.Config
	Br     *bufio.Reader
}

// NewProtocolReader readers an instance of a protocol reader
func NewProtocolReader(config *config.Config) *ProtocolReader {
	return &ProtocolReader{
		config: config,
		Br:     bufio.NewReaderSize(bytes.NewReader([]byte("")), 1024*8),
	}
}

func (pr *ProtocolReader) ReadCommand(r io.Reader) (*Command, error) {
	pr.Br.Reset(r)

	line, err := ReadLine(pr.Br)
	if err != nil {
		return nil, err
	}
	internal.Debugf(pr.config, "read(raw): %q", line)

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
		line, err = ReadLine(pr.Br)
		if err != nil {
			return nil, err
		}
		internal.Debugf(pr.config, "read arg(raw): %q", internal.Prettybuf(line))

		parts = bytes.SplitN(line, []byte(" "), 2)
		if len(parts) != 2 {
			return nil, errors.New("Badly formatted argument")
		}

		_, err := ParseNumber(parts[0])
		if err != nil {
			return nil, errors.New("Badly formatted argument length")
		}

		arg := parts[1]

		args = append(args, arg)
	}

	return NewCommand(pr.config, name, args...), nil
}

// ReadResponse reads a READ command response from an io.Reader
func (pr *ProtocolReader) ReadResponse(r io.Reader) (*Response, error) {
	pr.Br.Reset(r)
	line, err := ReadLine(pr.Br)
	if err != nil {
		return nil, err
	}

	parts := bytes.SplitN(line, []byte(" "), 2)
	var resp *Response
	if bytes.Equal(parts[0], []byte("OK")) {
		resp = NewResponse(pr.config, RespOK)
		if len(parts) > 1 {
			var n uint64

			subParts := bytes.SplitN(parts[1], []byte(" "), 2)

			_, err := fmt.Sscanf(string(subParts[0]), "%d", &n)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse response id or body length")
			}

			if len(subParts) > 1 { // this is an OK string response. ie from STATS

				rest := bytes.Join(subParts[1:], []byte(" "))
				rest = append(rest, []byte("\r\n")...)
				if uint64(len(rest)) < n {
					restBytes := make([]byte, n-uint64(len(rest)))
					if _, err := io.ReadFull(pr.Br, restBytes); err != nil {
						return nil, errors.Wrap(err, "failed to read OK body")
					}

					rest = append(rest, restBytes...)
				}

				resp.Body = rest
				if n != uint64(len(rest)) {
					return resp, errors.Wrap(errInvalidBodyLength, "failed to read OK body")
				}
			} else { // just want the ID
				resp.ID = n
			}
		}
	} else if bytes.Equal(parts[0], []byte("+EOF")) {
		resp = NewResponse(pr.config, RespEOF)
	} else if bytes.Equal(parts[0], []byte("ERR")) {
		var arg []byte
		if len(parts) > 1 {
			arg = parts[1]
		}
		resp = NewErrResponse(pr.config, arg)
	} else if bytes.Equal(parts[0], []byte("ERR_CLIENT")) {
		resp = NewClientErrResponse(pr.config, parts[1])
	} else {
		internal.Debugf(pr.config, "invalid response: %q", line)
		return nil, errors.New("Invalid response")
	}

	return resp, nil
}

// ReadLine reads a line from a bufio.Reader
func ReadLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, ProtocolError("long response line")
	}
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrap(err, "reading line failed")
	}

	if len(line) < termLen {
		return nil, ProtocolError("line missing terminator")
	}

	if line[len(line)-1] != '\n' || line[len(line)-2] != '\r' {
		return nil, ProtocolError("bad response line terminator")
	}

	line = line[:len(line)-2]
	return line, nil
}

// ProtocolError is a client error type
type ProtocolError string

func (pe ProtocolError) Error() string {
	return fmt.Sprintf("%s (possible server error)", string(pe))
}
