package logd

import (
	"io"
)

// Logger handles reading from and writing to the log
type Logger interface {
	logWriter
	logReader
}

type logWriter interface {
	io.Writer
	Flush() error
}

type logReader interface {
	// TODO this should take a ReaderFrom instead of a channel. Want to be able
	// to read the log file into the connection.
	// ReadFromID(rf io.ReaderFrom, id uint64, limit int) error
	ReadFromID(c chan []byte, id uint64, limit int) error
	Head() (uint64, error)
}

type logReplicator interface {
	readFromID(id uint64) (int64, error)
}
