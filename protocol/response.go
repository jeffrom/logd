package protocol

import (
	"bytes"
	"io"
	"log"

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
	ErrRespInvalid      = []byte("invalid request")
	ErrRespEmptyMessage = []byte("empty message not allowed")
	ErrRespNoArguments  = []byte("must supply an argument")
	ErrRespNotFound     = []byte("not found")
	ErrRespServer       = []byte("internal error")
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
	r := &Response{
		config:  conf,
		Status:  status,
		ReaderC: make(chan io.Reader, 1000),
	}
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

// SprintBytes returns a byte representation of the response
func (r *Response) SprintBytes() ([]byte, error) {
	w := NewProtocolWriter()
	return w.writeResponse(r)
}

func (r *Response) String() string {
	b, _ := r.SprintBytes()
	return string(b)
}

func (r *Response) Failed() bool {
	return r.Status != RespOK && r.Status != RespEOF
}

func (r *Response) SendBytes(b []byte) {
	internal.Debugf(r.config, "<-ReaderC %q", b)
	reader := bytes.NewReader(b)
	r.ReaderC <- reader
}

func (r *Response) SendEOF() {
	w := NewProtocolWriter()
	b, err := w.writeResponse(NewResponse(r.config, RespEOF))
	if err != nil {
		// NOTE shouldn't ever happen, just being pedantic
		log.Printf("error writing protocol response: %+v", err)
		return
	}
	internal.Debugf(r.config, "<-ReaderC %q (with flush)", b)
	reader := newFlushReader(bytes.NewReader(b))
	r.ReaderC <- reader
}
