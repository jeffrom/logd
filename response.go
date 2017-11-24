package logd

import (
	"bytes"
	"fmt"
	"io"
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
	errRespNoArguments  = []byte("must supply an argument")
	errRespInvalid      = []byte("invalid request")
	errRespEmptyMessage = []byte("empty message not allowed")
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
	config  *Config
	Status  RespType
	ID      uint64
	body    []byte
	readerC chan io.Reader
}

func newResponse(config *Config, status RespType) *Response {
	r := &Response{config: config, Status: status}
	return r
}

// NewErrResponse returns a new server error response
func NewErrResponse(config *Config, body []byte) *Response {
	return &Response{config: config, Status: RespErr, body: body}
}

// NewClientErrResponse returns a new validation error reponse
func NewClientErrResponse(config *Config, body []byte) *Response {
	return &Response{config: config, Status: RespErrClient, body: body}
}

// Bytes returns a byte representation of the response
func (r *Response) Bytes() []byte {
	return newProtocolWriter().writeResponse(r)
}

func (r *Response) String() string {
	return string(r.Bytes())
}

func (r *Response) sendChunk(lf logReadableFile) {
	size, limit := lf.SizeLimit()
	buflen := size
	if limit > 0 {
		buflen = limit
	}

	debugf(r.config, "<-readerC %d byte chunk", buflen)
	reader := bytes.NewReader([]byte(fmt.Sprintf("+%d\r\n", buflen)))
	r.readerC <- reader
	r.readerC <- io.LimitReader(lf.AsFile(), buflen)
}

func (r *Response) sendBytes(b []byte) {
	debugf(r.config, "<-readerC %q (response)", b)
	reader := bytes.NewReader(b)
	r.readerC <- reader
}
