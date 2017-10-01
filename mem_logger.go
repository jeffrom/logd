package logd

import (
	"errors"
	"io"
)

type memLogger struct {
	messages      [][]byte
	headC         chan []byte
	returnErr     bool
	headReturnErr error
	discard       bool
	currID        uint64
}

func newMemLogger() *memLogger {
	return &memLogger{headC: make(chan []byte, 1024)}
}

func (l *memLogger) Write(b []byte) (int, error) {
	if l.returnErr {
		return 0, errors.New("hey it's an error")
	}
	if !l.discard {
		cp := make([]byte, len(b))
		copy(cp, b)
		l.messages = append(l.messages, cp)
	}

	return len(b), nil
}

func (l *memLogger) SetID(id uint64) {
	l.currID = id
}

func (l *memLogger) Flush() error {
	if l.returnErr {
		return errors.New("hey it's an error")
	}
	return nil
}

func (l *memLogger) Read(b []byte) (int, error) {
	var out []byte
	id := l.currID
	currID := id
	if id == 0 {
		id = 1
	}
	for i := id - 1; i < uint64(len(l.messages)); i++ {
		msgBytes := make([]byte, len(l.messages[i]))
		copy(msgBytes, l.messages[i])
		out = append(out, msgBytes...)
		currID++
	}
	l.currID = currID

	for i, ch := range out {
		b[i] = ch
	}
	if len(out) == 0 {
		return 0, io.EOF
	}
	return len(out), nil
}

func (l *memLogger) SeekToID(id uint64) error {
	if l.returnErr {
		return errors.New("hey it's an error")
	}
	l.currID = id
	return nil
}

func (l *memLogger) Head() (uint64, error) {
	if l.headReturnErr != nil {
		return 0, l.headReturnErr
	}

	return uint64(len(l.messages)), nil
}

func (l *memLogger) Copy() Logger {
	ml := newMemLogger()
	ml.messages = l.messages
	return ml
}
