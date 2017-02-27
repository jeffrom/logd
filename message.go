package logd

type message struct {
	id   uint64
	body []byte
}

func newMessage(id uint64, body []byte) *message {
	return &message{id: id, body: body}
}
