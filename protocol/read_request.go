package protocol

import "github.com/jeffrom/logd/config"

// ReadRequest is an incoming READ/2 command
// READV2 <offset> <limit>\r\n
type ReadRequest struct {
	conf   *config.Config
	offset uint64
	limit  int
}

// NewReadRequest returns a new instance of *ReadRequest
func NewReadRequest(conf *config.Config) *ReadRequest {
	return &ReadRequest{
		conf: conf,
	}
}

func (r *ReadRequest) reset() {
	r.offset = 0
	r.limit = 0
}

// FromRequest parses a request, populating the ReadRequest. If validation
// fails, an error is returned
func (r *ReadRequest) FromRequest(req *Request) (*ReadRequest, error) {
	if req.nargs != argLens[CmdReadV2] {
		return r, errInvalidNumArgs
	}

	n, err := asciiToUint(req.args[0])
	if err != nil {
		return r, err
	}
	r.offset = n

	n, err = asciiToUint(req.args[1])
	if err != nil {
		return r, err
	}
	r.limit = int(n)
	return r, nil
}

// Offset returns the requested offset
func (r *ReadRequest) Offset() uint64 {
	return r.offset
}

// Limit returns the requested limit
func (r *ReadRequest) Limit() int {
	return r.limit
}
