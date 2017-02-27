package logd

import "fmt"

type respType uint8

const (
	_ respType = iota

	respOK
	respErr
)

func (resp *respType) String() string {
	switch *resp {
	case respOK:
		return "OK"
	case respErr:
		return "ERR"
	}
	return fmt.Sprintf("<unknown_resp %v>", *resp)
}

// response is returned to the caller
type response struct {
	status respType
	id     uint64
	body   []byte
	msgC   chan *message
}

func newResponse(status respType) *response {
	r := &response{status: status}
	return r
}
