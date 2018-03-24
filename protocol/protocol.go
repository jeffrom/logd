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
	"fmt"
	"hash/crc32"
	"io"

	"github.com/pkg/errors"
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
