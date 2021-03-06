package protocol

import (
	"io"

	"github.com/jeffrom/logd/config"
)

// Read represents a read request
// READ <topic> <offset> <messages>\r\n
type Read struct {
	conf     *config.Config
	Offset   uint64
	Messages int
	topic    []byte
	ntopic   int
	digitbuf [32]byte
}

// NewRead returns a new instance of a READ request
func NewRead(conf *config.Config) *Read {
	r := &Read{
		conf:  conf,
		topic: make([]byte, MaxTopicSize),
	}

	return r
}

// Reset puts READ in an initial state so it can be reused
func (r *Read) Reset() {
	r.Offset = 0
	r.Messages = 0
	r.ntopic = 0
}

// SetTopic sets the topic for a batch.
func (r *Read) SetTopic(topic []byte) {
	copy(r.topic, topic)
	r.ntopic = len(topic)
}

// Topic returns the topic for the batch.
func (r *Read) Topic() string {
	return string(r.TopicSlice())
}

// TopicSlice returns the topic for the batch as a byte slice. The byte slice
// is not copied.
func (r *Read) TopicSlice() []byte {
	return r.topic[:r.ntopic]
}

// FromRequest parses a request, populating the Read struct. If validation
// fails, an error is returned
func (r *Read) FromRequest(req *Request) (*Read, error) {
	if req.nargs != argLens[CmdRead] {
		return r, errInvalidNumArgs
	}

	r.SetTopic(req.args[0])

	n, err := asciiToUint(req.args[1])
	if err != nil {
		return r, err
	}
	r.Offset = n

	n, err = asciiToUint(req.args[2])
	if err != nil {
		return r, err
	}
	r.Messages = int(n)

	return r, r.Validate()
}

// Validate checks the READ arguments are valid
func (r *Read) Validate() error {
	if r.Messages < 1 {
		return ErrInvalid
	}
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

	n, err = w.Write(r.TopicSlice())
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
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
