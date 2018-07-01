package logger

type flusher interface {
	Flush() error
}
