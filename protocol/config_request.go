package protocol

import (
	"io"

	"github.com/jeffrom/logd/config"
)

// ConfigRequest is an incoming CONFIG command
// CONFIG\r\n
type ConfigRequest struct {
	conf *config.Config
}

// NewConfigRequest returns a new instance of ConfigRequest
func NewConfigRequest(conf *config.Config) *ConfigRequest {
	return &ConfigRequest{
		conf: conf,
	}
}

// Reset sets the ConfigRequest to its initial values
func (r *ConfigRequest) Reset() {

}

// FromRequest parses a request, populating the ReadRequest
func (r *ConfigRequest) FromRequest(req *Request) (*ConfigRequest, error) {
	if req.nargs > 0 {
		return r, errInvalidNumArgs
	}
	return r, nil
}

func (r *ConfigRequest) WriteTo(w io.Writer) (int64, error) {
	var total int64

	n, err := w.Write(bconfig)
	total += int64(n)
	if err != nil {
		return int64(total), err
	}

	return total, nil
}
