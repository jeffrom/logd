package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/jeffrom/logd/config"
)

var respBytes = map[error][]byte{
	ErrNotFound:            []byte("not found"),
	ErrInvalid:             ErrRespInvalid,
	errTooLarge:            []byte(errTooLarge.Error()),
	errInvalidProtocolLine: []byte("invalid protocol"),
	errCrcChecksumMismatch: []byte("checksum mismatch"),
}

func parseError(p []byte) error {
	if len(p) == 0 {
		return ErrInternal
	}
	if bytes.Equal(p, respBytes[ErrNotFound]) {
		return ErrNotFound
	}
	if bytes.Equal(p, respBytes[ErrInvalid]) {
		return ErrInvalid
	}
	if bytes.Equal(p, respBytes[errInvalidProtocolLine]) {
		return errInvalidProtocolLine
	}
	if bytes.Equal(p, respBytes[errCrcChecksumMismatch]) {
		return errCrcChecksumMismatch
	}
	return ErrInternal
}

// ClientResponse is the response clients receive after making a request.
// There are a few possible responses:
// OK <offset>\r\n
// BATCH <size> <checksum> <messages>\r\n<data>...
// MOK <size>\r\n<body>\r\n
// ERR <reason>\r\n
// ERR\r\n
type ClientResponse struct {
	conf     *config.Config
	offset   uint64
	err      error
	mokBuf   []byte
	digitbuf [32]byte
}

// NewClientResponse creates a new instance of *ClientResponse
func NewClientResponse(conf *config.Config) *ClientResponse {
	return &ClientResponse{
		conf: conf,
	}
}

// NewClientBatchResponse returns a successful batch *ClientResponse
func NewClientBatchResponse(conf *config.Config, off uint64) *ClientResponse {
	cr := NewClientResponse(conf)
	cr.SetOffset(off)
	return cr
}

// NewClientMultiResponse returns a successful MOK response
func NewClientMultiResponse(conf *config.Config, p []byte) *ClientResponse {
	cr := NewClientResponse(conf)
	cr.SetMultiResp(p)
	return cr
}

// NewClientErrResponse returns an error response
func NewClientErrResponse(conf *config.Config, err error) *ClientResponse {
	cr := NewClientResponse(conf)
	cr.SetError(err)
	return cr
}

func (cr *ClientResponse) String() string {
	return fmt.Sprintf("ClientResponse<offset: %d, err: %v>", cr.offset, cr.err)
}

// Reset sets ClientResponse to initial values
func (cr *ClientResponse) Reset() {
	cr.offset = 0
	cr.err = nil
	cr.mokBuf = nil
}

// SetOffset sets the offset number for a batch response
func (cr *ClientResponse) SetOffset(off uint64) {
	cr.offset = off
}

// Offset returns the response offset. It will panic if the response type isn't
// for a batch.
func (cr *ClientResponse) Offset() uint64 {
	return cr.offset
}

// SetError sets the error on the response
func (cr *ClientResponse) SetError(err error) {
	cr.err = err
}

func (cr *ClientResponse) Error() error {
	return cr.err
}

// SetMultiResp sets the MOK response body
func (cr *ClientResponse) SetMultiResp(p []byte) {
	cr.mokBuf = p
}

// MultiResp returns the responses MOK response body
func (cr *ClientResponse) MultiResp() []byte {
	return cr.mokBuf
}

// WriteTo implements io.WriterTo
func (cr *ClientResponse) WriteTo(w io.Writer) (int64, error) {
	if cr.err != nil {
		return cr.writeERR(w)
	}
	if cr.mokBuf != nil {
		return cr.writeMOK(w)
	}
	return cr.writeOK(w)
}

func (cr *ClientResponse) writeMOK(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(bmokStart)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(len(cr.mokBuf)), &cr.digitbuf)
	n, err = w.Write(cr.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(cr.mokBuf)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}
	return total, nil
}

func (cr *ClientResponse) writeERR(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(berr)
	total += int64(n)
	if err != nil {
		return total, err
	}

	if p, ok := respBytes[cr.err]; ok {
		n, err = w.Write(bspace)
		total += int64(n)
		if err != nil {
			return total, err
		}

		n, err = w.Write(p)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}
	return total, nil
}

func (cr *ClientResponse) writeOK(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(bok)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(cr.offset), &cr.digitbuf)
	n, err = w.Write(cr.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// ReadFrom implements io.ReaderFrom
func (cr *ClientResponse) ReadFrom(r io.Reader) (int64, error) {
	if br, ok := r.(*bufio.Reader); ok {
		return cr.readFromBuf(br)
	}
	return cr.readFromBuf(bufio.NewReader(r))
}

func (cr *ClientResponse) readFromBuf(r *bufio.Reader) (int64, error) {
	var total int64

	line, err := r.ReadSlice('\n')
	total += int64(len(line))
	if err != nil {
		return total, err
	}

	var word []byte
	line, word, err = parseWord(line)
	if err != nil {
		return total, err
	}

	var isErr bool
	var isMok bool
	if bytes.HasPrefix(word, berr) {
		isErr = true
	} else if bytes.HasPrefix(word, bmok) {
		isMok = true
	} else if !bytes.HasPrefix(word, bok) {
		return total, errInvalidProtocolLine
	}

	if isErr {
		errBytes := line
		if len(line) > 2 && line[len(line)-1] == '\n' && line[len(line)-2] == '\r' {
			errBytes = line[:len(line)-termLen]
		}
		cr.err = parseError(errBytes)
	} else if isMok {
		panic("MOK read not implemented")
	} else {
		_, word, err = parseWord(line)
		if err != nil {
			return total, err
		}

		n, perr := asciiToUint(word)
		err = perr
		if err != nil {
			return total, err
		}
		cr.offset = n
	}

	return total, err
}
