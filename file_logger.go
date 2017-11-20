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
		parts:  newFilePartitions(config),
	}

	return l
}

func (l *fileLogger) Setup() error {
	if l.config.LogFile != "" {
		err := l.parts.setCurrentFileHandles(false)
		if err != nil {
			return errors.Wrap(err, "failed to get file handles")
		}

		l.getNewIndex()
		if _, err := l.index.loadFromReader(); err != nil {
			return errors.Wrap(err, "failed to load index file")
		}
		// if l.config.Verbose {
		// 	debugf(l.config, "Current index")
		// 	l.index.dump()
		// }

		if err := l.loadState(); err != nil {
			log.Printf("error loading state: %+v\n", err)
			return err
		}

		log.Printf("Starting at log id %d, partition %d, offset %d", l.currID, l.parts.head(), l.written)
	}
	return nil
}

func (l *fileLogger) Shutdown() error {
	debugf(l.config, "shutting down file logger")
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
	debugf(l.config, "LOG <- %s (%d)", prettybuf(bytes.Trim(b, "\n")), len(b))

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

// TODO break this out into a Read and ReadPartition function, the latter of
// which stops on the current partition. we need this to accurately seek
func (l *fileLogger) Read(b []byte) (int, error) {
	var err error
	var read int
	for {
		var n int
		n, err = l.parts.r.Read(b[read:])
		// fmt.Printf("total buf:\n\"%s\"curr: \"%s\"\nerr: %v\n", b, b[read:], err)
		read += n
		if err == io.EOF {
			if l.parts.currReadPart < l.parts.head() {
				if oerr := l.parts.setReadHandle(l.parts.currReadPart + 1); oerr != nil {
					return read, oerr
				}
				debugf(l.config, "switched to partition %d", l.parts.currReadPart)
				err = nil
			} else {
				return read, err
			}
		}

		if err != nil {
			return read, err
		}
	}
}

func (l *fileLogger) SeekToID(id uint64) error {
	part, offset := l.index.Get(id)
	debugf(l.config, "index.Get(%d) -> (part %d, offset %d)", id, part, offset)

	if err := l.parts.setReadHandle(part); err != nil {
		return errors.Wrap(err, "failed setting read handle")
	}

	if _, err := l.parts.r.Seek(int64(offset), io.SeekStart); err != nil {
		return errors.Wrap(err, "failed seeking in partition")
	}

	if id <= 1 {
		return nil
	}

	var scanner *protocolScanner

Loop:
	for {
		scanner = newProtocolScanner(l.config, l.parts.r)

		for scanner.Scan() {
			msg := scanner.Message()
			// fmt.Println(scanner.Error(), msg.String())
			if msg.ID == id-1 {
				break Loop
			}
			if msg.ID > id-1 {
				return errors.New("failed to scan to id")
			}
		}

		if err := scanner.Error(); err == io.EOF {
			if l.parts.currReadPart < l.parts.head() {
				if oerr := l.parts.setReadHandle(l.parts.currReadPart + 1); oerr != nil {
					return oerr
				}
				continue
			} else {
				return err
			}
		} else if err != nil {
			panic(err)
		}

	}

	off := int64(offset) + int64(scanner.chunkPos) - int64(scanner.lastChunkPos)
	debugf(l.config, "seeking to offset %d, partition %d", off, l.parts.currReadPart)
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

	var msg *Message
	scanner := newProtocolScanner(l.config, l)
	for scanner.Scan() {
		if scanned := scanner.Message(); scanned != nil {
			msg = scanned
		}
	}
	err := scanner.Error()
	if err == io.EOF {
		err = nil
	}

	if msg == nil {
		return 0, err
	}
	return msg.ID, err
}

func (l *fileLogger) Copy() Logger {
	return newFileLogger(l.config)
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
	scanner := newProtocolScanner(l.config, l)
	var lastMsg *Message
	for scanner.Scan() {
		msg := scanner.Message()
		if msg != nil {
			lastMsg = msg
		}
	}
	if err := scanner.Error(); err != nil && err != io.EOF {
		return err
	}

	// TODO make written a uint64
	l.written = int(scanner.chunkPos)
	if lastMsg != nil {
		l.currID = lastMsg.ID + 1
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

		scanner := newProtocolScanner(logger.config, logger.parts.r)
		scanner.Scan()
		if err := scanner.Error(); err != nil {
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
