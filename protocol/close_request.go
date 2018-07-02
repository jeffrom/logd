package protocol

import (
	"io"

	"github.com/jeffrom/logd/config"
)

// CloseRequest is an incoming STATS command
// CLOSE\r\n
type CloseRequest struct {
	conf *config.Config
}

// NewCloseRequest returns a new instance of CloseRequest
func NewCloseRequest(conf *config.Config) *CloseRequest {
	return &CloseRequest{
		conf: conf,
	}
}

// Reset sets the CloseRequest to its initial values
func (r *CloseRequest) Reset() {

}

// FromRequest parses a request, populating the ReadRequest
func (r *CloseRequest) FromRequest(req *Request) (*CloseRequest, error) {
	if req.nargs > 0 {
		return r, errInvalidNumArgs
	}
	return r, nil
}

// WriteTo implements io.WriterTo
func (r *CloseRequest) WriteTo(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(bclose)
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
