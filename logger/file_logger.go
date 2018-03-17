package logger

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/pkg/errors"
)

// FileLogger manages reading and writing to the log partitions
type FileLogger struct {
	config  *config.Config
	parts   *filePartitions
	index   *fileIndex
	written int
	currID  uint64
}

type flusher interface {
	Flush() error
}

func NewFileLogger(conf *config.Config) *FileLogger {
	l := &FileLogger{
		config: conf,
		parts:  newFilePartitions(conf),
	}

	return l
}

// Setup loads to current state from disk
func (l *FileLogger) Setup() error {
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
		// 	internal.Debugf(l.config, "Current index")
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

// Shutdown cleans up file handles
func (l *FileLogger) Shutdown() error {
	internal.Debugf(l.config, "shutting down file logger")
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

func (l *FileLogger) Write(b []byte) (int, error) {
	internal.Debugf(l.config, "LOG <- %s (%d)", internal.Prettybuf(bytes.Trim(b, "\n")), len(b))

	written := l.written + len(b)
	madeNewPartition := false
	if written > l.config.PartitionSize {
		if err := l.parts.setCurrentFileHandles(true); err != nil {
			return 0, err
		}

		internal.Debugf(l.config, "Moved to partition %d", l.parts.head())
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

		internal.Debugf(l.config, "Writing to index, id: %d, partition: %d, offset: %d", id, part, offset)

		if _, werr := l.index.Append(id, part, offset); werr != nil {
			return written, errors.Wrap(werr, "Failed to write to index")
		}
	}

	l.written = written
	// fmt.Println("write", l.currID, n, l.written)
	return n, err
}

// SetID sets the current log id
func (l *FileLogger) SetID(id uint64) {
	l.currID = id
}

// Flush flushes the current partition to disk
func (l *FileLogger) Flush() error {
	if fl, ok := l.parts.w.(flusher); ok {
		return fl.Flush()
	}
	return nil
}

func (l *FileLogger) Read(b []byte) (int, error) {
	var err error
	var read int
	for {
		var n int
		n, err = l.parts.r.Read(b[read:])
		// fmt.Printf("total buf(%d):\n\"%s\"curr: \"%s\"\nerr: %v\n", len(b), b, b[read:], err)
		read += n
		if err == nil && read >= len(b) {
			return read, nil
		}
		if err == io.EOF {
			if l.parts.currReadPart < l.parts.head() {
				if oerr := l.parts.setReadHandle(l.parts.currReadPart + 1); oerr != nil {
					return read, oerr
				}
				internal.Debugf(l.config, "switched to partition %d", l.parts.currReadPart)
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

// SeekToID seeks to the partition and offset starting with id
func (l *FileLogger) SeekToID(id uint64) error {
	_, off, err := l.getPartOffset(id, false)
	if err != nil {
		return err
	}
	internal.Debugf(l.config, "seeking to offset %d, partition %d", off, l.parts.currReadPart)
	if _, err := l.parts.r.Seek(off, io.SeekStart); err != nil {
		return errors.Wrap(err, "failed to seek in partition after scanning")
	}

	return nil
}

func (l *FileLogger) getPartOffset(id uint64, inclusive bool) (uint64, int64, error) {
	if l.index == nil {
		if err := l.getNewIndex(); err != nil {
			return 0, 0, errors.Wrap(err, "failed to create index")
		}
	}
	// catch up any new indexes written
	if _, err := l.index.loadFromReader(); err != nil {
		return 0, 0, errors.Wrap(err, "failed to initialize index")
	}

	part, offset := l.index.Get(id)

	if err := l.parts.setReadHandle(part); err != nil {
		// the matching message may be on the next partition
		part++
		if nperr := l.parts.setReadHandle(part); nperr != nil {
			return part, 0, errors.Wrap(nperr, "failed setting read handle")
		}
		offset = 0
	} else {
		if _, err := l.parts.r.Seek(int64(offset), io.SeekStart); err != nil {
			return part, 0, errors.Wrap(err, "failed seeking in partition")
		}
	}

	if id <= 1 {
		return 0, 0, nil
	}

	var scanner *protocol.ProtocolScanner

Loop:
	for {
		scanner = protocol.NewProtocolScanner(l.config, l.parts.r)

		for scanner.Scan() {
			msg := scanner.Message()
			// fmt.Println("scanning for id", id, scanner.Error(), msg.String())
			if msg.ID == id {
				break Loop
			}
			if msg.ID > id {
				return part, 0, errors.New("failed to scan to id")
			}
		}

		if err := scanner.Error(); err == io.EOF {
			if l.parts.currReadPart < l.parts.head() {
				if oerr := l.parts.setReadHandle(l.parts.currReadPart + 1); oerr != nil {
					return part, 0, errors.Wrap(oerr, "failed to set read handle")
				}

				offset = 0
				part++
				continue
			} else {
				break
				// return part, 0, io.EOF
				// return part, 0, errors.Wrap(err, "failed scanning")
			}
		} else if err != nil {
			panic(err)
		}

	}

	off := int64(offset) + int64(scanner.ChunkPos)
	if !inclusive {
		off -= int64(scanner.LastChunkPos)
	}
	// fmt.Println(offset, scanner.ChunkPos, scanner.LastChunkPos)
	internal.Debugf(l.config, "looked up id %d: partition: %d, offset: %d", id, part, off)
	return part, off, nil
}

// Head returns the current latest ID
func (l *FileLogger) Head() (uint64, error) {
	curr := l.parts.head()
	if err := l.parts.setReadHandle(curr); err != nil {
		return 0, err
	}
	if _, err := l.parts.r.Seek(0, 0); err != nil {
		return 0, err
	}

	var msg *protocol.Message
	scanner := protocol.NewProtocolScanner(l.config, l)
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

// Copy returns a copy of the current logger. the copy needs to be set up again
// to load current log state.
func (l *FileLogger) Copy() Logger {
	return NewFileLogger(l.config)
}

// Range returns an iterator that feeds log messages to the caller.
func (l *FileLogger) Range(start, end uint64) (LogRangeIterator, error) {
	internal.Debugf(l.config, "Range(%d, %d)", start, end)

	lcopy := NewFileLogger(l.config)
	defer lcopy.Shutdown()
	endpart, endoff, pcerr := lcopy.getPartOffset(end, false)
	if pcerr != nil {
		return nil, errors.Wrap(pcerr, "failed to get range upper bound")
	}

	currpart, curroff, perr := l.getPartOffset(start, false)
	if perr != nil {
		return nil, errors.Wrap(perr, "failed to get range lower bound")
	}

	if err := l.parts.setReadHandle(currpart); err != nil {
		return nil, errors.Wrap(err, "failed to make new read handle after range")
	}
	if _, serr := l.parts.r.Seek(curroff, io.SeekStart); serr != nil {
		return nil, errors.Wrap(serr, "failed to seek log in range query")
	}

	r := l.parts.r
	l.parts.r = nil
	if err := l.parts.setReadHandle(l.parts.currReadPart); err != nil {
		return nil, errors.Wrap(err, "failed to make new read handle after range")
	}

	var lf LogReadableFile
	fn := func() (LogReadableFile, error) {
		if lf != nil {
			currpart++
			if currpart > endpart || currpart > l.parts.head() {
				return nil, io.EOF
			}
			curroff = 0

			nextpath := l.parts.logFilePath(currpart)
			f, err := os.Open(nextpath)
			if err != nil {
				return nil, errors.Wrap(err, "failed to open next file in range")
			}

			r = newLogFile(f)
		}
		lf = r

		if currpart == endpart {
			lf.SetLimit(endoff - curroff)
		} else {
			stat, err := lf.AsFile().Stat()
			internal.PanicOnError(err)
			lf.SetLimit(stat.Size() - curroff)
		}

		// use the current partition, which is seeked to `start`
		return lf, nil
	}
	return PartitionIteratorFunc(fn), nil
}

type PartitionIterator struct {
	next func() (LogReadableFile, error)
	err  error
	lf   LogReadableFile
}

func PartitionIteratorFunc(next func() (LogReadableFile, error)) *PartitionIterator {
	return &PartitionIterator{next: next}
}

func (pi *PartitionIterator) Next() bool {
	lf, err := pi.next()
	pi.lf = lf
	if err != nil {
		pi.err = err
		return false
	}
	return true
}

func (pi *PartitionIterator) Error() error {
	return pi.err
}

func (pi *PartitionIterator) LogFile() LogReadableFile {
	return pi.lf
}

func closeAll(closers []func() error) error {
	var firstErr error
	for _, closer := range closers {
		if err := closer(); err != nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *FileLogger) getNewIndex() error {
	idxFileName := l.config.IndexFileName()
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

func (l *FileLogger) loadState() error {
	scanner := protocol.NewProtocolScanner(l.config, l)
	var lastMsg *protocol.Message
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
	l.written = int(scanner.ChunkPos)
	if lastMsg != nil {
		l.currID = lastMsg.ID + 1
	}

	return nil
}

// CheckIndex verifies the index against the log file
func CheckIndex(config *config.Config) error {
	logger := NewFileLogger(config)
	defer logger.Shutdown()
	if err := logger.Setup(); err != nil {
		return err
	}

	for _, c := range logger.index.data {
		internal.Debugf(config, "checking id %d, partition %d, offset %d", c.id, c.part, c.offset)
		if err := logger.parts.setHandles(c.part); err != nil {
			log.Printf("Failed to set partition. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}
		if _, err := logger.parts.r.Seek(int64(c.offset), io.SeekStart); err != nil {
			log.Printf("Failed to seek to offset. id: %d, partition: %d, offset: %d", c.id, c.part, c.offset)
			return err
		}

		scanner := protocol.NewProtocolScanner(logger.config, logger.parts.r)
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

func currSize(r io.Reader) int64 {
	info, err := r.(*os.File).Stat()
	internal.PanicOnError(err)
	return info.Size()
}
