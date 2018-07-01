package protocol

import "github.com/jeffrom/logd/config"

// StatsRequest is an incoming STATS command
// STATSV2\r\n
type StatsRequest struct {
	conf *config.Config
}

// NewStatsRequest returns a new instance of StatsRequest
func NewStatsRequest(conf *config.Config) *StatsRequest {
	return &StatsRequest{
		conf: conf,
	}
}

// Reset sets the StatsRequest to its initial values
func (r *StatsRequest) Reset() {

}

// FromRequest parses a request, populating the ReadRequest
func (r *StatsRequest) FromRequest(req *Request) (*StatsRequest, error) {
	if req.nargs > 0 {
		return r, errInvalidNumArgs
	}
	return r, nil
}
