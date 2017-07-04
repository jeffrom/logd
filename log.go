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

// TODO read methods should take a ReaderFrom instead of a []byte. Want to
// be able to read the log file into the connection.
// ReadFromID(rf io.ReaderFrom, id uint64, limit int) error
type logReader interface {
	io.Reader
	SeekToID(id uint64) error
	Head() (uint64, error)
}

type logManager interface {
	Setup() error
	Shutdown() error
}

type logReplicator interface {
	readFromID(id uint64) (int64, error)
}

type logReadableFile interface {
	io.ReadSeeker
	io.Closer
}

type logWriteableFile interface {
	io.Writer
	io.Closer
}

type logIndexFile interface {
	io.ReadWriteSeeker
	io.Closer
}
