package logd

import (
	"errors"
)

type memLogger struct {
	messages  [][]byte
	headC     chan []byte
	returnErr bool
}

func newMemLogger() *memLogger {
	return &memLogger{headC: make(chan []byte, 1024*1024)}
}

func (log *memLogger) WriteMessage(msg []byte) (int, uint64, error) {
	if log.returnErr {
		return 0, 0, errors.New("hey it's an error")
	}
	log.messages = append(log.messages, msg)

	id, err := log.Head()
	if err != nil {
		return 0, 0, err
	}

	return len(msg), id, nil
}

func (log *memLogger) ReadFromID(c chan *message, id uint64, limit int) error {
	if log.returnErr {
		return errors.New("hey it's an error")
	}
	if id == 0 {
		id = 1
	}
	forever := limit == 0

	// TODO read forever in a goroutine that can be closed
	for i := id - 1; i < uint64(len(log.messages)); i++ {
		c <- newMessage(i+1, log.messages[i])

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
	if log.returnErr {
		return 0, errors.New("hey it's an error")
	}

	return uint64(len(log.messages)), nil
}
