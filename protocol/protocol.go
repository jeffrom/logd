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

// ReadLine reads a line from a bufio.Reader
func ReadLine(br *bufio.Reader) (int, []byte, error) {
	line, err := br.ReadSlice('\n')
	n := len(line) + 1
	if err == bufio.ErrBufferFull {
		return n, nil, Error("long response line")
	}
	if err == io.EOF {
		return n, nil, err
	}
	if err != nil {
		return n, nil, errors.Wrap(err, "reading line failed")
	}

	if len(line) < termLen {
		return n, nil, Error("line missing terminator")
	}

	if line[len(line)-1] != '\n' || line[len(line)-2] != '\r' {
		return n, nil, Error("bad response line terminator")
	}

	line = line[:len(line)-2]
	return n, line, nil
}

// Error is a client error type
type Error string

func (pe Error) Error() string {
	return fmt.Sprintf("%s (possible server error)", string(pe))
}

func trimNewline(line []byte) []byte {
	if line == nil || len(line) < 1 {
		return line
	}
	if line[len(line)-1] == '\n' {
		line = line[len(line):]
	}
	if line[len(line)-1] == '\r' {
		line = line[len(line):]
	}
	return line
}

func readWord(line []byte) ([]byte, []byte, error) {
	n := bytes.IndexAny(line, " \n")
	if n < 0 {
		return line, nil, errors.New("invalid bytes")
	}
	word := line[:n]
	if word[n-1] == '\r' {
		word = line[:n]
	}
	return line[n+1:], word, nil
}

func isDigits(b []byte) bool {
	if b == nil || len(b) == 0 {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] < 48 || b[i] > 57 {
			return false
		}
	}
	return true
}

func readInt(line []byte) ([]byte, int64, error) {
	if line == nil || len(line) == 0 {
		return nil, 0, errors.New("invalid bytes")
	}
	n := bytes.IndexAny(line, " \n")
	if n <= 0 {
		if !isDigits(line) {
			return line, 0, errors.New("invalid bytes")
		}
		n = len(line) - 1
	}
	numb := line[:n]
	if len(line) == 1 {
		numb = line
	}
	if len(numb) > 1 && numb[n-1] == '\r' {
		numb = line[:n]
	}
	num, err := asciiToInt(numb)
	return line[n+1:], num, err
}

func readUint(line []byte) ([]byte, uint64, error) {
	if line == nil || len(line) == 0 {
		return nil, 0, errors.New("invalid bytes")
	}
	n := bytes.IndexAny(line, " \n")
	if n < 0 {
		if !isDigits(line) {
			return line, 0, errors.New("invalid bytes")
		}
		n = len(line) - 1
	}
	numb := line[:n]
	if len(line) == 1 {
		numb = line
	}
	if len(numb) > 1 && numb[n-1] == '\r' {
		numb = line[:n]
	}
	num, err := asciiToUint(numb)
	return line[n+1:], num, err
}

func asciiToUint(tok []byte) (uint64, error) {
	var n uint64
	for i := 0; i < len(tok); i++ {
		ch := tok[i]
		if ch < 48 || ch > 57 {
			return 0, errors.New("invalid byte")
		}
		n = (n * 10) + uint64(ch-'0')
	}
	return n, nil
}

func asciiToInt(tok []byte) (int64, error) {
	var n int64
	for i := 0; i < len(tok); i++ {
		ch := tok[i]
		if ch < 48 || ch > 57 {
			return 0, errors.New("invalid byte")
		}
		n = (n * 10) + int64(ch-'0')
	}
	return n, nil
}

func intToASCII(n int64, b *[32]byte) int {
	i := 31
	if n < 0 {
		b[i] = '-'
		i--
	}
	for n > 9 {
		digit := n % 10
		n /= 10
		b[i] = '0' + byte(digit)
		i--
	}
	return i
}

func uintToASCII(n uint64, b *[32]byte) int {
	i := 31
	for n > 9 {
		digit := n % 10
		n /= 10
		b[i] = '0' + byte(digit)
		i--
	}
	b[i] = '0' + byte(n)
	i--

	return i + 1
}
