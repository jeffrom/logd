package logd

// Logger handles reading from and writing to the log
type Logger interface {
	WriteMessage(msg []byte) (int, uint64, error)
	ReadFromID(c chan *message, id uint64, limit int) error
	Head() (uint64, error)
}
