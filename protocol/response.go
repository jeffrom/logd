package protocol

import (
	"errors"
	"io"

	"github.com/jeffrom/logd/config"
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
	// ErrNotFound is returned when the log offset is above the logs head, has been
	// deleted, or does not point to a message batch.
	ErrNotFound = errors.New("offset not found")

	// ErrInternal is a server side error. This is an "ERR" response from the
	// server.
	ErrInternal = errors.New("internal server error")

	// ErrInvalid refers to an invalid request.
	ErrInvalid = errors.New("invalid request")

	// errTooLarge is returned when the batch size is larger than the
	// configured max batch size.
	errTooLarge = errors.New("too large")

	//
	// protocol responses
	//

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

	// ErrRespTooLarge indicates a protocol error
	ErrRespTooLarge = []byte("too large")
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

// Response is a response the conn can use to send bytes back to the client.
// can returns bytes as well as *os.File-s
type Response struct {
	conf       *config.Config
	readers    []io.ReadCloser
	numReaders int
	numScanned int
}

// NewResponse returns a new response
func NewResponse(conf *config.Config) *Response {
	return &Response{
		conf:    conf,
		readers: make([]io.ReadCloser, conf.MaxPartitions+1),
	}
}

// NewResponseErr returns a new instance of Response and writes it to the request
func NewResponseErr(conf *config.Config, req *Request, err error) (*Response, error) {
	clientResp := NewClientErrResponse(conf, err)
	resp := NewResponse(conf)
	if _, werr := req.WriteResponse(resp, clientResp); werr != nil {
		return resp, werr
	}
	return resp, err
}

// Reset sets the response to its initial values
func (r *Response) Reset() {
	for i := 0; i < r.numReaders; i++ {
		r.readers[i] = nil
	}
	r.numReaders = 0
	r.numScanned = 0
}

// AddReader adds a reader for the server to send back over the conn
func (r *Response) AddReader(rdr io.ReadCloser) error {
	if r.numReaders > r.conf.MaxPartitions+1 {
		panic("too many readers in response")
	}
	r.readers[r.numReaders] = rdr
	r.numReaders++
	return nil
}

// ScanReader returns the next reader, or io.EOF if they've all been scanned
func (r *Response) ScanReader() (io.ReadCloser, error) {
	if r.numScanned > r.numReaders {
		return nil, io.EOF
	}

	rdr := r.readers[r.numScanned]
	r.numScanned++
	return rdr, nil
}

// NumReaders returns the number of io.Readers available
func (r *Response) NumReaders() int {
	return r.numReaders
}
