package logd

type subscription struct {
	msgC chan *message
	done chan struct{}
}

func newSubscription(msgC chan *message, done chan struct{}) *subscription {
	return &subscription{
		msgC: msgC,
		done: done,
	}
}

func (subs *subscription) send(msg *message) {
	subs.msgC <- msg
}

func (subs *subscription) finish() {
	select {
	case subs.done <- struct{}{}:
	default:
	}
}
