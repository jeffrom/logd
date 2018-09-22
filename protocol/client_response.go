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
	errNoTopic:             []byte("request missing topic"),
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
	if bytes.Equal(p, respBytes[errNoTopic]) {
		return errNoTopic
	}
	return ErrInternal
}

// ClientResponse is the response clients receive after making a request.
// There are a few possible responses:
// OK\r\n
// OK <offset> <batches>\r\n
// BATCH <size> <checksum> <messages>\r\n<data>...
// MOK <size>\r\n<body>\r\n
// ERR <reason>\r\n
// ERR\r\n
type ClientResponse struct {
	conf     *config.Config
	ok       bool
	offset   uint64
	nbatches int
	err      error
	mokBuf   []byte
	mokSize  int
	nmok     int
	digitbuf [32]byte
}

func NewClientResponse() *ClientResponse { return &ClientResponse{} }

func (cr *ClientResponse) WithConfig(conf *config.Config) *ClientResponse {
	cr.conf = conf
	return cr
}

// NewClientResponseConfig creates a new instance of *ClientResponse
func NewClientResponseConfig(conf *config.Config) *ClientResponse {
	return NewClientResponse().WithConfig(conf)
}

// NewClientBatchResponse returns a successful batch *ClientResponse
func NewClientBatchResponse(conf *config.Config, off uint64, batches int) *ClientResponse {
	cr := NewClientResponseConfig(conf)
	cr.SetOffset(off)
	cr.SetBatches(batches)
	return cr
}

// NewClientOKResponse returns a successful batch *ClientResponse
func NewClientOKResponse(conf *config.Config) *ClientResponse {
	cr := NewClientResponseConfig(conf)
	cr.SetOK()
	return cr
}

// NewClientMultiResponse returns a successful MOK response
func NewClientMultiResponse(conf *config.Config, p []byte) *ClientResponse {
	cr := NewClientResponseConfig(conf)
	cr.SetMultiResp(p)
	return cr
}

// NewClientErrResponse returns an error response
func NewClientErrResponse(conf *config.Config, err error) *ClientResponse {
	cr := NewClientResponseConfig(conf)
	cr.SetError(err)
	return cr
}

func (cr *ClientResponse) String() string {
	if cr.ok {
		return fmt.Sprintf("OK")
	}
	if cr.err != nil {
		return cr.err.Error()
	}
	if cr.mokBuf != nil {
		return fmt.Sprintf("MOK %d", len(cr.mokBuf))
	}
	if cr.nbatches == 0 {
		cr.nbatches = 1
	}
	return fmt.Sprintf("OK %d %d", cr.offset, cr.nbatches)
}

// Reset sets ClientResponse to initial values
func (cr *ClientResponse) Reset() {
	cr.offset = 0
	cr.nbatches = 0
	cr.err = nil
	cr.mokBuf = nil
	cr.ok = false
	cr.nmok = 0
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

// SetBatches sets the number of batches in an OK response
func (cr *ClientResponse) SetBatches(n int) {
	cr.nbatches = n
}

// Batches returns the number of batches in an OK response
func (cr *ClientResponse) Batches() int {
	return cr.nbatches
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

func (cr *ClientResponse) SetOK() {
	cr.ok = true
}

// Ok returns true if the request has succeeded
func (cr *ClientResponse) Ok() bool {
	return cr.ok
}

// MultiResp returns the responses MOK response body
func (cr *ClientResponse) MultiResp() []byte {
	return cr.mokBuf[:cr.nmok]
}

// WriteTo implements io.WriterTo
func (cr *ClientResponse) WriteTo(w io.Writer) (int64, error) {
	if cr.ok {
		return cr.writeOK(w)
	}
	if cr.err != nil {
		return cr.writeERR(w)
	}
	if cr.mokBuf != nil {
		return cr.writeMOK(w)
	}
	return cr.writeBatchOK(w)
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

func (cr *ClientResponse) writeBatchOK(w io.Writer) (int64, error) {
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

	l := uintToASCII(cr.offset, &cr.digitbuf)
	n, err = w.Write(cr.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	// writing batches is only one at a time, so the number of batches written
	// to the log from a request isn't calculated during writes. if it's 0,
	// just set it to one.
	// TODO events should probably do this. it may be better not to have this
	// calculation here at all for correctness sake
	if cr.nbatches == 0 {
		cr.nbatches = 1
	}
	l = uintToASCII(uint64(cr.nbatches), &cr.digitbuf)
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

func (cr *ClientResponse) writeOK(w io.Writer) (int64, error) {
	var total int64

	n, err := w.Write(bok)
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

	if bytes.Equal(line, bokResp) {
		cr.ok = true
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
		nmok, err := cr.readMOK(line, r)
		total += nmok
		if err != nil {
			return total, err
		}
	} else {
		line, word, err = parseWord(line)
		if err != nil {
			return total, err
		}

		n, perr := asciiToUint(word)
		err = perr
		if err != nil {
			return total, err
		}
		cr.offset = n

		_, word, err = parseWord(line)
		if err != nil {
			return total, err
		}

		n, perr = asciiToUint(word)
		err = perr
		if err != nil {
			return total, err
		}
		cr.nbatches = int(n)
	}

	return total, err
}

func (cr *ClientResponse) readMOK(line []byte, r *bufio.Reader) (int64, error) {
	_, word, err := parseWord(line)
	if err != nil {
		return 0, err
	}

	n, err := asciiToUint(word)
	if err != nil {
		return 0, err
	}
	cr.mokSize = int(n)

	if len(cr.mokBuf) < int(n) {
		cr.mokBuf = make([]byte, n)
	}

	read, err := io.ReadFull(r, cr.mokBuf[:int(n)])
	cr.nmok = read
	if err != nil {
		return int64(read), err
	}

	return int64(read), nil
}
