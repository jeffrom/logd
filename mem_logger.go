package logd

import (
	"errors"
)

type memLogger struct {
	messages      [][]byte
	headC         chan []byte
	returnErr     bool
	headReturnErr error
	discard       bool
}

func newMemLogger() *memLogger {
	return &memLogger{headC: make(chan []byte, 1024)}
}

func (log *memLogger) Write(b []byte) (int, error) {
	if log.returnErr {
		return 0, errors.New("hey it's an error")
	}
	if !log.discard {
		cp := make([]byte, len(b))
		copy(cp, b)
		log.messages = append(log.messages, cp)
	}

	return len(b), nil
}

func (log *memLogger) Flush() error {
	if log.returnErr {
		return errors.New("hey it's an error")
	}
	return nil
}

func (log *memLogger) ReadFromID(c chan []byte, id uint64, limit int) error {
	if log.returnErr {
		return errors.New("hey it's an error")
	}
	if id == 0 {
		id = 1
	}
	forever := limit == 0

	for i := id - 1; i < uint64(len(log.messages)); i++ {
		msgBytes := make([]byte, len(log.messages[i]))
		copy(msgBytes, log.messages[i])
		c <- msgBytes

		if !forever {
			limit--
			if limit == 0 {
				break
			}
		}
	}

	return nil
}

func (log *memLogger) Head() (uint64, error) {
	if log.headReturnErr != nil {
		return 0, log.headReturnErr
	}

	return uint64(len(log.messages)), nil
}
