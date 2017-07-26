package logd

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
)

type fileLogger struct {
	config  *Config
	parts   *filePartitions
	index   *fileIndex
	written int
	currID  uint64
}

type flusher interface {
	Flush() error
}

func newFileLogger(config *Config) *fileLogger {
	l := &fileLogger{
		config: config,
		parts:  newPartitions(config),
	}

	return l
}

func (l *fileLogger) Setup() error {
	if l.config.LogFile != "" {
		debugf(l.config, "Starting at partition %d", l.parts.head())
		err := l.parts.setCurrentFileHandles(false)
		if err != nil {
			return errors.Wrap(err, "failed to get file handles")
		}

		l.getNewIndex()
		if _, err := l.index.loadFromReader(); err != nil {
			return errors.Wrap(err, "failed to load index file")
		}
		if l.config.Verbose {
			debugf(l.config, "Current index")
			l.index.dump()
		}

		if err := l.loadState(); err != nil {
			return err
		}
	}
	return nil
}

func (l *fileLogger) Shutdown() error {
	var firstErr error
	if err := l.index.shutdown(); err != nil {
		firstErr = err
	}
	if err := l.parts.shutdown(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *fileLogger) Write(b []byte) (int, error) {
	debugf(l.config, "LOG <- %s", bytes.Trim(b, "\n"))

	written := l.written + len(b)
	madeNewPartition := false
	if written > l.config.PartitionSize {
		if err := l.parts.setCurrentFileHandles(true); err != nil {
			return 0, err
		}

		debugf(l.config, "Moved to partition %d", l.parts.head())
		l.written = 0
		madeNewPartition = true
	}

	n, err := l.parts.Write(b)
	if err != nil {
		return n, errors.Wrap(err, "failed to write to log")
	}
	if madeNewPartition {
		written = n
	}

	if l.currID > 0 && l.currID%l.config.IndexCursorSize == 0 {
		id := l.currID
		part := uint64(l.parts.head())
		offset := uint64(written - n)

		debugf(l.config, "Writing to index, id: %d, partition: %d, offset: %d", id, part, offset)

		if _, werr := l.index.Append(id, part, offset); werr != nil {
			return written, errors.Wrap(werr, "Failed to write to index")
		}
	}

	l.written = written
	// fmt.Println("write", l.currID, n, l.written)
	return n, err
}

func (l *fileLogger) SetID(id uint64) {
	l.currID = id
}

func (l *fileLogger) Flush() error {
	if fl, ok := l.parts.w.(flusher); ok {
		return fl.Flush()
	}
	return nil
}

func (l *fileLogger) Read(b []byte) (int, error) {
	var err error
	var read int
	for {
		var n int
		n, err = l.parts.r.Read(b)
		read += n
		if err == io.EOF {
			if l.parts.currReadPart < l.parts.head() {
				if oerr := l.parts.setReadHandle(l.parts.currReadPart + 1); oerr != nil {
					return read, oerr
				}
				debugf(l.config, "now using part %d", l.parts.currReadPart)
				continue
			} else {
				return read, err
			}
		}

		if n > 0 || err != io.EOF {
			if err == io.EOF && l.parts.currReadPart < l.parts.head() {
				err = nil // more readers remain
			}
			return read, err
		}
	}
}

func (l *fileLogger) SeekToID(id uint64) error {
	part, offset := l.index.Get(id)

	if err := l.parts.setReadHandle(part); err != nil {
		return errors.Wrap(err, "failed setting read handle")
	}

	if _, err := l.parts.r.Seek(int64(offset), io.SeekStart); err != nil {
		return errors.Wrap(err, "failed seeking in partition")
	}

	if id <= 1 {
		return nil
	}

	scanner := newFileLogScanner(l.config, l)
	for scanner.Scan() {
		msg := scanner.Message()
		if msg.id == id-1 {
			break
		}
		if msg.id > id-1 {
			return errors.New("failed to scan to id")
		}
	}

	off := int64(offset) + int64(scanner.read) - int64(scanner.lastRead)
	if _, err := l.parts.r.Seek(off, io.SeekStart); err != nil {
		return errors.Wrap(err, "failed to seek in partition after scanning")
	}

	return nil
}

func (l *fileLogger) Head() (uint64, error) {
	curr := l.parts.head()
	if err := l.parts.setReadHandle(curr); err != nil {
		return 0, err
	}
	if _, err := l.parts.r.Seek(0, 0); err != nil {
		return 0, err
	}

	scanner := newFileLogScanner(l.config, l)
	for scanner.Scan() {
	}
	msg := scanner.Message()
	if msg == nil {
		return 0, scanner.Error()
	}
	return msg.id, scanner.Error()
}

func (l *fileLogger) getNewIndex() error {
	idxFileName := l.config.indexFileName()
	w, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(l.config.LogFileMode))
	if err != nil {
		return errors.Wrap(err, "failed to open log index for writing")
	}

	rs, err := os.Open(idxFileName)
	if err != nil {
		return errors.Wrap(err, "failed to open log index for reading")
	}

	l.index = newFileIndex(l.config, w, rs)
	return err
}

func (l *fileLogger) loadState() error {
	// var c *fileIndexCursor
	// if len(l.index.data) > 0 {
	// 	c = l.index.data[len(l.index.data)-1]
	// }

	scanner := newFileLogScanner(l.config, l)
	for scanner.Scan() {
	}
	if err := scanner.Error(); err != nil {
		return err
	}

	msg := scanner.Message()
	if msg != nil {
		l.currID = msg.id + 1
		l.written = scanner.read

		log.Printf("Starting at log id %d, offset %d", l.currID, l.written)
	}

	return nil
}

// CheckIndex verifies the index against the log file
func CheckIndex(config *Config) error {
	logger := newFileLogger(config)
	defer logger.Shutdown()
	if err := logger.Setup(); err != nil {
		return err
	}

	for _, c := range logger.index.data {
		debugf(config, "checking id %d, partition %d, offset %d", c.id, c.part, c.offset)
		if err := logger.parts.setHandles(c.part); err != nil {
			log.Printf("Failed to set partition. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}
		if _, err := logger.parts.r.Seek(int64(c.offset), io.SeekStart); err != nil {
			log.Printf("Failed to seek to offset. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}

		scanner := newFileLogScanner(logger.config, logger.parts.r)
		scanner.Scan()
		if err := scanner.Error(); err != nil {
			log.Printf("Failed to scan. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}

		msg := scanner.Message()
		if msg.id != c.id {
			log.Printf("Read incorrect id at id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return fmt.Errorf("expected id %d but got %d", c.id, msg.id)
		}
	}
	return nil
}
