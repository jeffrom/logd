package logd

// Message is a log message type.
type Message struct {
	id   uint64
	body []byte
}

// NewMessage returns a new instance of a Message.
func NewMessage(id uint64, body []byte) *Message {
	return &Message{id: id, body: body}
}
