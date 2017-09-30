package logd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// Message is a log message type.
type Message struct {
	ID   uint64
	Body []byte
}

// NewMessage returns a new instance of a Message.
func NewMessage(id uint64, body []byte) *Message {
	return &Message{ID: id, Body: body}
}

func (m *Message) logBytes() []byte {
	return []byte(fmt.Sprintf("%d %d %s\r\n", m.ID, len(m.Body), m.Body))
}

func (m *Message) String() string {
	return string(m.logBytes())
}

func msgFromBytes(b []byte) (*Message, error) {
	var id uint64
	var length uint64
	var body []byte
	parts := bytes.SplitN(b, []byte(" "), 3)

	if len(parts) < 3 {
		return nil, errors.New("invalid message format")
	}

	_, err := fmt.Sscanf(string(parts[0]), "%d", &id)
	if err != nil {
		return nil, errors.Wrap(err, "invalid id bytes")
	}
	_, err = fmt.Sscanf(string(parts[1]), "%d", &length)
	if err != nil {
		return nil, errors.Wrap(err, "invalid length bytes")
	}
	body = bytes.TrimRight(parts[2], "\r\n")

	if uint64(len(body)) < length {
		return nil, errors.New("invalid body length")
	}

	// TODO slicing here is unsafe, we need a crc or something
	return NewMessage(id, body[:length]), nil
}

func msgFromReader(r *bufio.Reader) (int, *Message, error) {
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
	_, err = fmt.Sscanf(string(idBytes), "%d", &id)
	if err != nil {
		return read, nil, errors.Wrap(err, "invalid id bytes")
	}

	lenBytes, err := r.ReadBytes(' ')
	// fmt.Printf("length: %q\n", lenBytes)
	read += len(lenBytes)
	if err != nil {
		return read, nil, err
	}

	var length uint64
	_, err = fmt.Sscanf(string(lenBytes), "%d", &length)
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

	return read, NewMessage(id, bytes.TrimRight(buf, "\r\n")), nil
}
