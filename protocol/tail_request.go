package protocol

import (
	"io"

	"github.com/jeffrom/logd/config"
)

// Tail represents a TAIL request
// TAIL <topic> <messages>\r\n
type Tail struct {
	conf     *config.Config
	Messages int
	topic    []byte
	ntopic   int
	digitbuf [32]byte
}

// NewTail returns a new instance of a TAIL request
func NewTail(conf *config.Config) *Tail {
	return &Tail{
		conf:  conf,
		topic: make([]byte, MaxTopicSize),
	}
}

// Reset puts TAIL in an initial state so it can be reused
func (t *Tail) Reset() {
	t.Messages = 0
	t.ntopic = 0
}

// SetTopic sets the topic of the TAIL request
func (t *Tail) SetTopic(topic []byte) {
	copy(t.topic, topic)
	t.ntopic = len(topic)
}

// Topic returns the topic as a string
func (t *Tail) Topic() string {
	return string(t.TopicSlice())
}

// TopicSlice returns the topic as a byte slice reference. It is not copied.
func (t *Tail) TopicSlice() []byte {
	return t.topic[:t.ntopic]
}

// FromRequest parses a request, populating the Tail struct. If validation
// fails, an error is returned.
func (t *Tail) FromRequest(req *Request) (*Tail, error) {
	if req.nargs != argLens[CmdTail] {
		return t, errInvalidNumArgs
	}

	t.SetTopic(req.args[0])

	n, err := asciiToUint(req.args[1])
	if err != nil {
		return t, err
	}
	t.Messages = int(n)
	return t, t.Validate()
}

// Validate checks the TAIL arguments are valid
func (t *Tail) Validate() error {
	if t.ntopic < 1 {
		return errNoTopic
	}
	return nil
}

// WriteTo implements io.WriterTo
func (t *Tail) WriteTo(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(btailStart)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(t.TopicSlice())
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
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
