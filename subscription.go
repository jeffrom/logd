package logd

// Subscription is used to tail logs
type Subscription struct {
	msgC chan *Message
	done chan struct{}
}

func newSubscription(msgC chan *Message, done chan struct{}) *Subscription {
	return &Subscription{
		msgC: msgC,
		done: done,
	}
}

func (subs *Subscription) send(msg *Message) {
	subs.msgC <- msg
}

func (subs *Subscription) finish() {
	select {
	case subs.done <- struct{}{}:
	default:
	}
}
