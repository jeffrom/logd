package logger

import (
	"bytes"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Log manages reading and writing to the log partitions
type Log struct {
	other   *Log
	config  *config.Config
	parts   *filePartitions
	index   *fileIndex
	written int
	currID  uint64
}

type flusher interface {
	Flush() error
}

// New returns a new instance of file logger
func New(conf *config.Config) *Log {
	l := &Log{
		config: conf,
		other:  newFileLoggerCopy(conf),
		index:  newFileIndex(conf),
		parts:  newFilePartitions(conf),
	}

	return l
}

func newFileLoggerCopy(conf *config.Config) *Log {
	return &Log{
		config: conf,
		index:  newFileIndex(conf),
		parts:  newFilePartitions(conf),
	}
}

// Reset resets the logger to its initial state
func (l *Log) Reset() {
	l.index.reset()
	l.parts.reset()
	l.currID = 0
	l.written = 0
}

// Setup loads to current state from disk
func (l *Log) Setup() error {
	if l.config.LogFile == "" {
		panic("config: LogFile not set")
	}

	err := l.parts.setCurrentFileHandles(false)
	if err != nil {
		return errors.Wrap(err, "failed to get file handles")
	}

	if err := l.index.setup(); err != nil {
		return err
	}
	// fmt.Printf("%+v\n", l.index)

	if l.config.Verbose {
		internal.Debugf(l.config, "Current index")
		l.index.dump()
	}

	// TODO on startup we need to make sure index head is the latest id

	if err := l.loadState(); err != nil {
		log.Printf("error loading state: %+v\n", err)
		return err
	}

	head, _ := l.parts.head()
	log.Printf("Starting at log id %d, partition %d, offset %d", l.currID, head, l.written)

	if l.other != nil {
		return l.other.Setup()
	}
	return nil
}

// Shutdown cleans up file handles
func (l *Log) Shutdown() error {
	internal.Debugf(l.config, "shutting down file logger")
	var firstErr error
	if err := l.index.shutdown(); err != nil {
		log.Printf("error shutting down index: %+v", err)
		firstErr = err
	}
	if err := l.parts.shutdown(); err != nil {
		log.Printf("error shutting down partitions: %+v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	if l.other != nil {
		other := l.other
		files := []io.Closer{
			other.index.r, other.index.w, other.index.hw,
			other.parts.r, other.parts.w,
		}
		if err := internal.CloseAll(files); err != nil {
			log.Printf("error shutting down other logger: %+v", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (l *Log) Write(b []byte) (int, error) {
	internal.Debugf(l.config, "LOG <- %s (%d)", internal.Prettybuf(bytes.Trim(b, "\n")), len(b))

	head, err := l.parts.head()
	if err != nil {
		return 0, err
	}

	written := l.written + len(b)
	madeNewPartition := false
	if written > l.config.PartitionSize {
		fherr := l.parts.setCurrentFileHandles(true)
		if fherr != nil {
			return 0, fherr
		}

		parts, perr := l.parts.partitions()
		if perr != nil {
			return 0, perr
		}

		maxParts := l.config.MaxPartitions
		if maxParts > 0 && len(parts) > maxParts {
			tailPart := parts[1]
			msg, merr := l.parts.firstMessage(tailPart)
			if merr != nil && merr != io.EOF {
				return 0, merr
			}

			if msg != nil && msg.ID > 0 {
				l.index.tail = msg.ID
			}

			// TODO move the file synchronously first, then any hook acts on the
			// moved file
			extraParts := parts[:len(parts)-maxParts]
			go l.parts.remove(extraParts)
		}

		internal.Debugf(l.config, "Moved to partition %d", head)
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

	l.index.head = l.currID
	if l.index.tail == 0 {
		l.index.tail = 1
	}
	if _, herr := l.index.writeHeader(); herr != nil {
		return written, errors.Wrap(herr, "failed to write index header")
	}

	if l.currID > 0 && l.currID%l.config.IndexCursorSize == 0 {
		head, err = l.parts.head()
		if err != nil {
			return 0, err
		}

		id := l.currID
		part := uint64(head)
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
func (l *Log) SetID(id uint64) {
	l.currID = id
}

// Flush flushes the current partition to disk
func (l *Log) Flush() error {
	if fl, ok := l.parts.w.(flusher); ok {
		if err := fl.Flush(); err != nil {
			return err
		}
	}
	return l.index.bw.Flush()
}

func (l *Log) Read(b []byte) (int, error) {
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
			head, herr := l.parts.head()
			if herr != nil {
				return read, herr
			}

			if l.parts.currReadPart < head {
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
func (l *Log) SeekToID(id uint64) error {
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

func (l *Log) getPartOffset(id uint64, inclusive bool) (uint64, int64, error) {
	// fmt.Printf("\ngetPartOffset: %+v\n", l.index)
	if id == 0 {
		return 0, 0, errors.New("0 passed to getPartOffset")
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

	if id == 1 && !inclusive {
		return 0, 0, nil
	}

	scanner := protocol.NewScanner(l.config, l.parts.r)

Loop:
	for {
		scanner.Reset(l.parts.r)

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
			head, herr := l.parts.head()
			if herr != nil {
				return part, 0, errors.Wrap(herr, "failed to find head")
			}
			if l.parts.currReadPart < head {
				if oerr := l.parts.setReadHandle(l.parts.currReadPart + 1); oerr != nil {
					return part, 0, errors.Wrap(oerr, "failed to set read handle")
				}

				offset = 0
				part++
				continue
			} else {
				break
			}
		} else if err != nil {
			return part, 0, errors.Wrap(err, "failed to scan log")
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
func (l *Log) Head() (uint64, error) {
	return l.index.head, nil
}

// Tail returns the lowest id in the log
func (l *Log) Tail() (uint64, error) {
	// return l.index.tail, nil

	part, herr := l.parts.tail()
	if herr != nil {
		return 0, herr
	}

	if err := l.parts.setReadHandle(part); err != nil {
		return 0, err
	}
	if _, err := l.parts.r.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var msg *protocol.Message
	scanner := protocol.NewScanner(l.config, l)
	for scanner.Scan() {
		if scanned := scanner.Message(); scanned != nil {
			msg = scanned
		}
		break
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
func (l *Log) Copy() Logger {
	return New(l.config)
}

// Range returns an iterator that feeds log messages to the caller.
func (l *Log) Range(start, end uint64) (LogRangeIterator, error) {
	internal.Debugf(l.config, "Range(%d, %d)", start, end)

	lcopy := l.other
	lcopy.Reset()
	if err := lcopy.Setup(); err != nil {
		return nil, err
	}

	endpart, endoff, pcerr := lcopy.getPartOffset(end, true)
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

	internal.Debugf(l.config, "Range: start %d:%d end %d:%d", currpart, curroff, endpart, endoff)

	var lf LogReadableFile
	fn := func() (LogReadableFile, error) {
		if lf != nil {
			currpart++
			head, herr := l.parts.head()
			if herr != nil {
				return nil, errors.Wrap(herr, "failed to get log head")
			}
			if currpart > endpart || currpart > head {
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

		limit := endoff - curroff
		if currpart == endpart {
			lf.SetLimit(limit)
		} else {
			stat, err := lf.AsFile().Stat()
			if err != nil {
				return nil, errors.Wrap(err, "failed to stat file")
			}
			limit = stat.Size() - curroff
			lf.SetLimit(limit)
		}

		internal.Debugf(l.config, "Range part %d: start %d end %d limit %d", currpart, curroff, endoff, limit)

		// use the current partition, which is seeked to `start`
		return lf, nil
	}
	return PartitionIteratorFunc(fn), nil
}

// PartitionIterator scans log entries
type PartitionIterator struct {
	next func() (LogReadableFile, error)
	err  error
	lf   LogReadableFile
}

// PartitionIteratorFunc is an adapter to allow the use of a function as a
// PartitionIterator
func PartitionIteratorFunc(next func() (LogReadableFile, error)) *PartitionIterator {
	return &PartitionIterator{next: next}
}

// Next returns the next message from a PartitionIterator
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

// LogFile returns the LogReadableFile for a partition
func (pi *PartitionIterator) LogFile() LogReadableFile {
	return pi.lf
}

func (l *Log) loadState() error {
	head := l.index.head
	if head > 1 {
		if err := l.SeekToID(head - 1); err != nil {
			return err
		}
	}

	scanner := protocol.NewScanner(l.config, l)
	var lastMsg *protocol.Message
	var firstMsg *protocol.Message
	for scanner.Scan() {
		msg := scanner.Message()
		if firstMsg == nil && msg != nil {
			firstMsg = msg
		}
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

	if lastMsg != nil && l.index.head == 0 {
		l.index.head = lastMsg.ID
	}
	if firstMsg != nil && l.index.tail == 0 {
		l.index.tail = firstMsg.ID
	}

	return nil
}
