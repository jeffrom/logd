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
const maxCRCSize = 10

var errInvalidFirstByte = errors.New("invalid first byte")
var errInvalidNumArgs = errors.New("invalid number of arguments")
var errReadStopped = errors.New("read stopped by other side")
var errInvalidProtocolLine = errors.New("invalid protocol line")
var errInvalidBodyLength = errors.New("invalid body length")
var errCrcChecksumMismatch = errors.New("crc checksum mismatch")
var errRangeNotFound = errors.New("id range not found")

var crcTable = crc32.MakeTable(crc32.Koopman)

var bnewLine = []byte("\r\n")
var bspace = []byte(" ")
var bmsg = []byte("MSG")
var bmsgStart = []byte("MSG ")
var bbatchStart = []byte("BATCH ")
var breadStart = []byte("READ ")
var btailStart = []byte("TAIL ")
var bok = []byte("OK")
var bokResp = []byte("OK\r\n")
var bokStart = []byte("OK ")
var berr = []byte("ERR")
var bmok = []byte("MOK")
var bmokStart = []byte("MOK ")
var bclose = []byte("CLOSE")

// ReadLine reads a line from a bufio.Reader
// NOTE the line data will be overwritten the next time the bufio.Reader is
// used.
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

func parseWord(line []byte) ([]byte, []byte, error) {
	n := bytes.IndexAny(line, " \n")
	if n < 0 {
		return line, nil, errors.New("invalid bytes")
	}
	word := line[:n]
	if word[n-1] == '\r' {
		word = line[:n-1]
	}
	return line[n+1:], word, nil
}

// same as parseWord, but returns the number of bytes read instead of the rest
// of the slice
func parseWordN(line []byte) (int, []byte, error) {
	n := bytes.IndexAny(line, " \n")
	if n < 0 {
		return 0, nil, errors.New("invalid bytes")
	}
	word := line[:n]
	if word[n-1] == '\r' {
		word = line[:n-1]
	}
	return n + 1, word, nil
}

func readWordFromBuf(r *bufio.Reader) (int64, []byte, []byte, error) {
	word, err := r.ReadSlice(' ')
	total := int64(len(word))
	return total, word[:len(word)-1], word, err
}

func readLineFromBuf(r *bufio.Reader) (int64, []byte, []byte, error) {
	word, err := r.ReadSlice('\n')
	total := int64(len(word))
	return total, word[:len(word)-2], word, err
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

func parseInt(line []byte) ([]byte, int64, error) {
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

func parseUint(line []byte) ([]byte, uint64, error) {
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

// XXX this is broken
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

func asciiSize(n int) int {
	size := 1
	if n < 0 {
		size *= -1
		size++
	}

	x := n
	for x > 9 {
		size++
		x /= 10
	}

	// fmt.Println("size of", n, size)
	return size
}

func readNewLine(r *bufio.Reader) (int, error) {
	ch, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if ch != '\r' {
		return 1, errors.Errorf("invalid first newline char: %q", ch)
	}
	ch, err = r.ReadByte()
	if err != nil {
		return 1, err
	}
	if ch != '\n' {
		return 2, errors.Errorf("invalid second newline char: %q", ch)
	}
	return 2, nil
}
