package protocol

import (
	"io"

	"github.com/jeffrom/logd/config"
)

// Tail represents a TAILV2 request
// TAILV2 <messages>\r\n
type Tail struct {
	conf     *config.Config
	Messages int
	digitbuf [32]byte
}

// NewTail returns a new instance of a TAILV2 request
func NewTail(conf *config.Config) *Tail {
	return &Tail{
		conf: conf,
	}
}

// Reset puts TAILV2 in an initial state so it can be reused
func (t *Tail) Reset() {
	t.Messages = 0
}

// FromRequest parses a request, populating the Tail struct. If validation
// fails, an error is returned.
func (t *Tail) FromRequest(req *Request) (*Tail, error) {
	if req.nargs != argLens[CmdTailV2] {
		return t, errInvalidNumArgs
	}

	n, err := asciiToUint(req.args[0])
	if err != nil {
		return t, err
	}
	t.Messages = int(n)
	return t, t.Validate()
}

// Validate checks the TAILV2 arguments are valid
func (t *Tail) Validate() error {
	return nil
}

// WriteTo implements io.WriterTo
func (t *Tail) WriteTo(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(btailv2Start)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(t.Messages), &t.digitbuf)
	n, err = w.Write(t.digitbuf[l:])
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
