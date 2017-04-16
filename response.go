package logd

import (
	"bytes"
	"strconv"
)

type respType uint8

const (
	_ respType = iota

	respOK
	respEOF
	respContinue
	respErr
	respErrClient
)

func (resp respType) String() string {
	switch resp {
	case respOK:
		return "OK"
	case respEOF:
		return "EOF"
	case respContinue:
		return "CONTINUE"
	case respErr:
		return "ERR"
	case respErrClient:
		return "ERR_CLIENT"
	}
	return "<unknown_resp_type>"
}

// Response is returned to the caller
type Response struct {
	status respType
	id     uint64
	body   []byte
	msgC   chan *Message
}

func newResponse(status respType) *Response {
	r := &Response{status: status}
	return r
}

func newErrResponse(body []byte) *Response {
	return &Response{status: respErr, body: body}
}

func newClientErrResponse(body []byte) *Response {
	return &Response{status: respErrClient, body: body}
}

// Bytes returns a byte representation of the response
func (r *Response) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteString(r.status.String())

	if r.id > 0 && r.body != nil {
		panic("response id and body both set")
	}

	if r.id != 0 {
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatUint(r.id, 10))
	}
	if r.body != nil {
		buf.WriteByte(' ')
		buf.Write(r.body)
	}

	buf.WriteString("\r\n")
	return buf.Bytes()
}

type scanner struct {
}

func newScanner() *scanner {
	return &scanner{}
}
