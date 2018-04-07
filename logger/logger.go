package logger

import (
	"fmt"
	"io"
	"log"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
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
	Head() (uint64, error)
	Tail() (uint64, error)
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
	AsFile() *internal.LogFile
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

// CheckIndex verifies the index against the log file
func CheckIndex(conf *config.Config) error {
	logger := New(conf)
	defer logger.Shutdown()
	if err := logger.Setup(); err != nil {
		return err
	}

	internal.Debugf(conf, "head: %d, tail: %d", logger.index.head, logger.index.tail)

	for _, c := range logger.index.data {
		internal.Debugf(conf, "checking id %d, partition %d, offset %d", c.id, c.part, c.offset)
		if err := logger.parts.setHandles(c.part); err != nil {
			log.Printf("Failed to set partition. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}
		if _, err := logger.parts.r.Seek(int64(c.offset), io.SeekStart); err != nil {
			log.Printf("Failed to seek to offset. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}

		scanner := protocol.NewScanner(logger.config, logger.parts.r)
		scanner.Scan()
		if err := scanner.Error(); err != nil {
			if err == io.EOF {
				continue
			}
			log.Printf("Failed to scan. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}

		msg := scanner.Message()
		if msg.ID != c.id {
			log.Printf("Read incorrect id at id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return fmt.Errorf("expected id %d but got %d", c.id, msg.ID)
		}
	}
	return nil
}
