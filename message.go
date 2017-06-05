package logd

import (
	"bytes"
	"errors"
	"fmt"
)

// Message is a log message type.
type Message struct {
	id   uint64
	body []byte
}

// NewMessage returns a new instance of a Message.
func NewMessage(id uint64, body []byte) *Message {
	return &Message{id: id, body: body}
}

func fromBytes(b []byte) (*Message, error) {
	var id uint64
	var length uint64
	var body []byte
	parts := bytes.SplitN(b, []byte(" "), 3)

	_, err := fmt.Sscanf(string(parts[0]), "%d", &id)
	if err != nil {
		return nil, err
	}
	_, err = fmt.Sscanf(string(parts[1]), "%d", &length)
	if err != nil {
		return nil, err
	}
	body = bytes.TrimRight(parts[2], "\r\n")

	if uint64(len(body)) != length {
		return nil, errors.New("invalid body length")
	}

	return NewMessage(id, body), nil
}

func (m *Message) bytes() []byte {
	return []byte(fmt.Sprintf("%d %d %s\r\n", m.id, len(m.body), m.body))
}
