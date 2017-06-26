package logd

import (
	"bytes"
	"strconv"
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
	RespContinue

	// RespErr indicates a failed response.
	RespErr

	// RespErrClient indicates a failed response due to client error.
	RespErrClient
)

func (resp RespType) String() string {
	switch resp {
	case RespOK:
		return "OK"
	case RespEOF:
		return "EOF"
	case RespContinue:
		return "CONTINUE"
	case RespErr:
		return "ERR"
	case RespErrClient:
		return "ERR_CLIENT"
	}
	return "<unknown_resp_type>"
}

// Response is returned to the caller
type Response struct {
	Status RespType
	ID     uint64
	body   []byte
	msgC   chan []byte
}

func newResponse(status RespType) *Response {
	r := &Response{Status: status}
	return r
}

func newErrResponse(body []byte) *Response {
	return &Response{Status: RespErr, body: body}
}

func newClientErrResponse(body []byte) *Response {
	return &Response{Status: RespErrClient, body: body}
}

// Bytes returns a byte representation of the response
func (r *Response) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteString(r.Status.String())

	if r.ID > 0 && r.body != nil {
		panic("response id and body both set")
	}

	if r.ID != 0 {
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatUint(r.ID, 10))
	}
	if r.body != nil {
		buf.WriteByte(' ')
		buf.Write(r.body)
	}

	buf.WriteString("\r\n")
	return buf.Bytes()
}
