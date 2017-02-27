package logd

// Logger handles reading from and writing to the log
type Logger interface {
	WriteMessage(msg []byte) (int, uint64, error)
	ReadFromID(c chan *message, id uint64, limit int) error
	Head() (uint64, error)
}

type memLogger struct {
	messages [][]byte
	headC    chan []byte
}

func newMemLogger() *memLogger {
	return &memLogger{headC: make(chan []byte, 0)}
}

func (log *memLogger) WriteMessage(msg []byte) (int, uint64, error) {
	log.messages = append(log.messages, msg)
	id, err := log.Head()
	if err != nil {
		return 0, 0, err
	}
	return len(msg), id, nil
}

func (log *memLogger) ReadFromID(c chan *message, id uint64, limit int) error {
	if id == 0 {
		id = 1
	}
	forever := limit == 0

	for i := id - 1; i < uint64(len(log.messages)); i++ {
		c <- newMessage(i+1, log.messages[i])
		if !forever {
			limit--
			if limit == 0 {
				break
			}
		}
	}

	if forever {
		currID, err := log.Head()
		if err != nil {
			return err
		}
		for msg := range log.headC {
			c <- newMessage(currID, msg)
			currID++
		}
	}
	return nil
}

func (log *memLogger) Head() (uint64, error) {
	return uint64(len(log.messages)), nil
}
