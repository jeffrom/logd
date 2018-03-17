package logger

import (
	"io"
	"os"

	"github.com/jeffrom/logd/protocol"
)

// Logger handles reading from and writing to the log
type Logger interface {
	LogWriter
	LogReader
}

// LogWriter implements methods for writing to log partitions.
type LogWriter interface {
	io.Writer
	Flush() error
	SetID(id uint64)
}

// LogReader should implement everything needed to read a range of messages
// TODO read methods should take a ReaderFrom instead of a []byte. Want to
// be able to read the log file into the connection.
// ReadFromID(rf io.ReaderFrom, id uint64, limit int) error
type LogReader interface {
	io.Reader
	SeekToID(id uint64) error
	Head() (uint64, error)
	Copy() Logger
	Range(start, end uint64) (LogRangeIterator, error)
}

type LogScannable interface {
	Scanner() LogScanner
}

type LogManager interface {
	Setup() error
	Shutdown() error
}

type LogReplicator interface {
	readFromID(id uint64) (int64, error)
}

type LogReadableFile interface {
	io.ReadSeeker
	io.Closer
	SetLimit(limit int64)
	SizeLimit() (int64, int64, error)
	AsFile() *os.File
}

type LogWriteableFile interface {
	io.Writer
	io.Closer
}

type LogIndexFile interface {
	io.ReadWriteSeeker
	io.Closer
}

type LogScanner interface {
	Scan() bool
	Message() *protocol.Message
	Error() error
}

// LogRanger implements methods needed to read ranges of ids from log
// partitions
type LogRanger interface {
	Range(start, end uint64) (LogRangeIterator, error)
}

// LogRangeIterator feeds log messages to the caller
type LogRangeIterator interface {
	Next() bool
	Error() error
	LogFile() LogReadableFile
}

type logFile struct {
	*os.File
	limit int64
}

func newLogFile(f *os.File) *logFile {
	return &logFile{File: f}
}

func (lf *logFile) SetLimit(limit int64) {
	lf.limit = limit
}

func (lf *logFile) SizeLimit() (int64, int64, error) {
	if lf.limit > 0 {
		return 0, lf.limit, nil
	}
	stat, err := lf.Stat()
	if err != nil {
		return -1, -1, err
	}
	return stat.Size(), 0, nil
}

func (lf *logFile) AsFile() *os.File {
	return lf.File
}
