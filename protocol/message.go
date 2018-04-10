package protocol

import (
	"bufio"
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/pkg/errors"
)

// Message is a log message type.
type Message struct {
	ID        uint64
	Body      []byte
	partition uint64
	offset    uint64
	checksum  uint32
	size      int
	allread   bool
}

// NewMessage returns a new instance of a Message.
func NewMessage(id uint64, body []byte) *Message {
	// fmt.Printf("NewMessage: %d -> %q\n", id, body)
	return &Message{
		ID:   id,
		Body: body,
	}
}

// Reset resets the message data
func (m *Message) Reset() {
	m.ID = 0
	m.Body = nil
	m.partition = 0
	m.offset = 0
	m.checksum = 0
	m.size = 0
	m.allread = false
}

func (m *Message) logBytes() []byte {
	b := NewWriter().Message(m)
	return []byte(b)
}

func (m *Message) String() string {
	return string(m.logBytes())
}

// MsgFromBytes loads the message from b
func MsgFromBytes(b []byte) (*Message, error) {
	ps := NewScanner(config.DefaultConfig, bytes.NewReader(b))
	_, msg, err := ps.ReadMessage()
	return msg, err
}

// MsgFromReader loads the message from r
func MsgFromReader(r io.Reader) (*Message, error) {
	ps := NewScanner(config.DefaultConfig, r)
	_, msg, err := ps.ReadMessage()
	return msg, err
}

func msgFromLogReader(r *bufio.Reader) (int, *Message, error) {
	var err error
	var n int
	var read int

	idBytes, err := r.ReadBytes(' ')
	read += len(idBytes)
	if err == io.EOF {
		return 0, nil, err
	}
	if err != nil {
		return 0, nil, errors.Wrap(err, "failed reading id bytes")
	}
	// fmt.Printf("id: %q\n", idBytes)

	var id uint64
	id, err = asciiToUint(idBytes)
	if err != nil {
		return read, nil, errors.Wrap(err, "invalid id bytes")
	}

	lenBytes, err := r.ReadBytes(' ')
	// fmt.Printf("length: %q\n", lenBytes)
	read += len(lenBytes)
	if err != nil {
		return read, nil, errors.Wrap(err, "failed reading length bytes")
	}

	var length uint64
	length, err = asciiToUint(lenBytes)
	if err != nil {
		return read, nil, errors.Wrap(err, "invalid length bytes")
	}

	buf := make([]byte, length+2)
	n, err = r.Read(buf)
	// fmt.Printf("msg: (%d) %q\n", length, buf)
	read += n
	if err != nil {
		return read, nil, errors.Wrap(err, "failed reading body")
	}

	return read, NewMessage(id, trimNewline(buf)), nil
}
