package logd

import "fmt"

// Subscription is used to tail logs
type Subscription struct {
	msgC chan []byte
	done chan struct{}
}

func newSubscription(msgC chan []byte, done chan struct{}) *Subscription {
	return &Subscription{
		msgC: msgC,
		done: done,
	}
}

func (subs *Subscription) send(msg []byte) {
	fmt.Printf("<-bytes %q (subscription)\n", msg)
	subs.msgC <- msg
}

func (subs *Subscription) finish() {
	select {
	case subs.done <- struct{}{}:
	default:
	}
}
