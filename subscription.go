package logd

import (
	"bytes"
	"io"
)

// Subscription is used to tail logs
type Subscription struct {
	readerC chan io.Reader
	done    chan struct{}
}

func newSubscription(readerC chan io.Reader, done chan struct{}) *Subscription {
	return &Subscription{
		readerC: readerC,
		done:    done,
	}
}

func (subs *Subscription) send(msg []byte) {
	// fmt.Printf("<-bytes %q (subscription)\n", prettybuf(msg))
	subs.readerC <- bytes.NewReader(msg)
}

func (subs *Subscription) finish() {
	select {
	case subs.done <- struct{}{}:
	default:
	}
}
