package protocol

import (
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/pkg/errors"
)

// RespType is the response status return type
type RespType uint8

const (
	_ RespType = iota

	// RespOK indicates a successful client request.
	RespOK

	// RespEOF indicates a client's read request has been closed by the
	// server.
	RespEOF

	// RespContinue indicates a read request has been closed but additional
	// requests can be handled. Not yet in use.
	// RespContinue

	// RespErr indicates a failed response.
	RespErr

	// RespErrClient indicates a failed response due to client error.
	RespErrClient
)

var (
	// ErrNotFound is returned when the log id is above the logs head or has been
	// deleted.
	ErrNotFound = errors.New("id not found")
)

var (
	// ErrRespInvalid are the error response bytes sent for invalid requests
	ErrRespInvalid = []byte("invalid request")

	// ErrRespEmptyMessage indicates a write was attempted that included no data
	ErrRespEmptyMessage = []byte("empty message not allowed")

	// ErrRespNoArguments indicates no arguments were supplied
	ErrRespNoArguments = []byte("must supply an argument")

	// ErrRespNotFound indicates the messages could not be found
	ErrRespNotFound = []byte("not found")

	// ErrRespServer indicates an internal server error
	ErrRespServer = []byte("internal error")
)

func (resp RespType) String() string {
	switch resp {
	case RespOK:
		return "OK"
	case RespEOF:
		return "EOF"
	// case RespContinue:
	// 	return "CONTINUE"
	case RespErr:
		return "ERR"
	case RespErrClient:
		return "ERR_CLIENT"
	}
	return "<unknown_resp_type>"
}

// Response is returned to the caller
type Response struct {
	config  *config.Config
	Status  RespType
	ID      uint64
	Body    []byte
	ReaderC chan ReadPart
	pw      *Writer
	eofbuf  *bytes.Reader
}

// NewResponse returns a new instance of a Response
func NewResponse(conf *config.Config, status RespType) *Response {
	r := &Response{
		config: conf,
		Status: status,
		pw:     NewWriter(),
		eofbuf: bytes.NewReader(nil),
	}
	return r
}

// NewErrResponse returns a new server error response
func NewErrResponse(conf *config.Config, body []byte) *Response {
	return &Response{config: conf, Status: RespErr, Body: body}
}

// NewClientErrResponse returns a new validation error response
func NewClientErrResponse(conf *config.Config, body []byte) *Response {
	return &Response{config: conf, Status: RespErrClient, Body: body}
}

// SprintBytes returns a byte representation of the response
func (r *Response) SprintBytes() ([]byte, error) {
	w := NewWriter()
	_, b := w.Response(r)
	return b, nil
}

func (r *Response) String() string {
	b, _ := r.SprintBytes()
	return string(b)
}

// Failed indicates whether the command should continue execution.
func (r *Response) Failed() bool {
	return r.Status != RespOK && r.Status != RespEOF
}

// SendBytes sends a PartReader back to the connection
func (r *Response) SendBytes(b []byte) {
	internal.Debugf(r.config, "<-ReaderC %q", b)
	reader := bytes.NewReader(b)
	r.ReaderC <- NewPartReader(reader)
}

// SendEOF sends an EOF to a connection, then finishes the response.
func (r *Response) SendEOF() {
	_, b := r.pw.EOF()
	internal.Debugf(r.config, "<-ReaderC %q (with flush)", b)
	r.eofbuf.Reset(b)
	r.ReaderC <- NewPartReader(r.eofbuf)
	r.ReaderC <- &PartDone{}
}

// response v2

const maxReaderPerResp = 50

// ResponseV2 is a response the conn can use to send bytes back to the client.
// can returns bytes as well as *os.File-s
type ResponseV2 struct {
	conf       *config.Config
	readers    []io.Reader
	numReaders int
	numScanned int
}

// NewResponseV2 returns a new response
func NewResponseV2(conf *config.Config) *ResponseV2 {
	return &ResponseV2{
		conf:    conf,
		readers: make([]io.Reader, maxReaderPerResp),
	}
}

// Reset sets the response to its initial values
func (r *ResponseV2) Reset() {
	for i := 0; i < r.numReaders; i++ {
		r.readers[i] = nil
	}
	r.numReaders = 0
	r.numScanned = 0
}

// AddReader adds a reader for the server to send back over the conn
func (r *ResponseV2) AddReader(rdr io.Reader) error {
	if r.numReaders > maxReaderPerResp {
		return errors.New("too many readers on this response")
	}
	r.readers[r.numReaders] = rdr
	r.numReaders++
	return nil
}

// ScanReader returns the next reader, or io.EOF if they've all been scanned
func (r *ResponseV2) ScanReader() (io.Reader, error) {
	if r.numScanned > r.numReaders {
		return nil, io.EOF
	}

	rdr := r.readers[r.numScanned]
	r.numScanned++
	return rdr, nil
}
