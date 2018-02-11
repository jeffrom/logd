package protocol

import (
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
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
	// ErrRespInvalid are the error response bytes sent for invalid requests
	ErrRespInvalid      = []byte("invalid request")
	ErrRespEmptyMessage = []byte("empty message not allowed")
	ErrRespNoArguments  = []byte("must supply an argument")
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
	ReaderC chan io.Reader
}

// NewResponse returns a new instance of a Response
func NewResponse(conf *config.Config, status RespType) *Response {
	r := &Response{config: conf, Status: status}
	return r
}

// NewErrResponse returns a new server error response
func NewErrResponse(conf *config.Config, body []byte) *Response {
	return &Response{config: conf, Status: RespErr, Body: body}
}

// NewClientErrResponse returns a new validation error reponse
func NewClientErrResponse(conf *config.Config, body []byte) *Response {
	return &Response{config: conf, Status: RespErrClient, Body: body}
}

// Bytes returns a byte representation of the response
func (r *Response) Bytes() []byte {
	return NewProtocolWriter().writeResponse(r)
}

func (r *Response) String() string {
	return string(r.Bytes())
}

// func (r *Response) sendChunk(lf logger.LogReadableFile) {
// 	size, limit := lf.SizeLimit()
// 	buflen := size
// 	if limit > 0 {
// 		buflen = limit
// 	}
// 	// buflen does not take seek position into account

// 	f := lf.AsFile()
// 	internal.Debugf(r.config, "<-%s: %d bytes", f.Name(), buflen)
// 	reader := bytes.NewReader([]byte(fmt.Sprintf("+%d\r\n", buflen)))
// 	r.ReaderC <- reader
// 	r.ReaderC <- io.LimitReader(f, buflen)
// }

func (r *Response) SendBytes(b []byte) {
	internal.Debugf(r.config, "<-ReaderC %q", b)
	reader := bytes.NewReader(b)
	r.ReaderC <- reader
}

func (r *Response) SendEOF() {
	b := NewProtocolWriter().writeResponse(NewResponse(r.config, RespEOF))
	internal.Debugf(r.config, "<-ReaderC %q (with flush)", b)
	reader := newFlushReader(bytes.NewReader(b))
	r.ReaderC <- reader
}
