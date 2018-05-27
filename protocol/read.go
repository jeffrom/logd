package protocol

import (
	"io"

	"github.com/jeffrom/logd/config"
)

// Read represents a read request
// READV2 <offset> <messages>\r\n
type Read struct {
	conf     *config.Config
	Offset   uint64
	Messages int
	digitbuf [32]byte
}

// NewRead returns a new instance of a READ request
func NewRead(conf *config.Config) *Read {
	r := &Read{
		conf: conf,
	}

	return r
}

// Reset puts READ in an initial state so it can be reused
func (r *Read) Reset() {

}

// FromRequest parses a request, populating the batch. If validation fails, an
// error is returned
func (r *Read) FromRequest(req *Request) (*Read, error) {
	if req.nargs != argLens[CmdReadV2] {
		return r, errInvalidNumArgs
	}

	n, err := asciiToUint(req.args[0])
	if err != nil {
		return r, err
	}
	r.Offset = n

	n, err = asciiToUint(req.args[1])
	if err != nil {
		return r, err
	}
	r.Messages = int(n)

	return r, nil
}

// Validate checks the READ arguments are valid
func (r *Read) Validate() error {
	return nil
}

// WriteTo implements io.WriterTo
func (r *Read) WriteTo(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(breadStart)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(r.Offset, &r.digitbuf)
	n, err = w.Write(r.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l = uintToASCII(uint64(r.Messages), &r.digitbuf)
	n, err = w.Write(r.digitbuf[l:])
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
