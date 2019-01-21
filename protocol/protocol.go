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
	stderrors "errors"
	"fmt"
	"hash/crc32"

	"github.com/pkg/errors"
)

const termLen = 2
const maxCRCSize = 10

var errInvalidFirstByte = stderrors.New("invalid first byte")
var errInvalidNumArgs = stderrors.New("invalid number of arguments")
var errReadStopped = stderrors.New("read stopped by other side")
var errInvalidProtocolLine = stderrors.New("invalid protocol line")
var errInvalidBodyLength = stderrors.New("invalid body length")
var errCrcMismatch = stderrors.New("crc checksum mismatch")

var crcTable = crc32.MakeTable(crc32.IEEE)

var bnewLine = []byte("\r\n")
var bspace = []byte(" ")
var bmsg = []byte("MSG")
var bmsgStart = []byte("MSG ")
var bbatchStart = []byte("BATCH ")
var breadStart = []byte("READ ")
var btailStart = []byte("TAIL ")
var bconfig = []byte("CONFIG\r\n")
var bok = []byte("OK")
var bokResp = []byte("OK\r\n")
var bokStart = []byte("OK ")
var berr = []byte("ERR")
var bmok = []byte("MOK")
var bmokStart = []byte("MOK ")
var bclose = []byte("CLOSE")

// Error is a client error type
type Error string

func (pe Error) Error() string {
	return fmt.Sprintf("%s (possible server error)", string(pe))
}

func parseWord(line []byte) ([]byte, []byte, error) {
	n := bytes.IndexAny(line, " \n")
	if n < 0 {
		return line, nil, errors.New("invalid bytes")
	}
	word := line[:n]
	if len(word) > 1 && word[n-1] == '\r' {
		word = line[:n-1]
	}
	return line[n+1:], word, nil
}

func readLineFromBuf(r *bufio.Reader) (int64, []byte, []byte, error) {
	word, err := r.ReadSlice('\n')
	total := int64(len(word))
	if err != nil {
		if total < 2 {
			return total, nil, word, err
		}
		return total, word[:len(word)-2], word, err
	}

	if word[len(word)-2] != '\r' {
		return total, word[:len(word)-2], word, errors.New("missing \r in newline")
	}
	return total, word[:len(word)-2], word, err
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
