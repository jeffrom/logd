package logd

type subscription struct {
	msgC chan *message
	// closeC chan struct{}
}

func newSubscription(msgC chan *message) *subscription {
	return &subscription{
		msgC: msgC,
		// closeC: make(chan struct{}, 0),
	}
}

func (subs *subscription) send(msg *message) {
	subs.msgC <- msg
}

// func (subs *subscription) close() {
// 	subs.closeC <- struct{}{}
// }
