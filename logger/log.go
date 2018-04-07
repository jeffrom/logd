package logger

import (
	"bytes"
	"io"
	"log"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Log manages reading and writing to the log partitions
type Log struct {
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
		index:  newFileIndex(conf),
		parts:  newFilePartitions(conf),
	}

	return l
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

	if err := l.index.setup(); err != nil {
		return err
	}

	if l.config.Verbose {
		internal.Debugf(l.config, "Current index")
		l.index.dump()
	}

	// fmt.Printf("%+v\n", l.index)
	if err := l.parts.setCurrentFileHandles(l.index.partHead); err != nil {
		return errors.Wrap(err, "failed to get file handles")
	}

	if err := l.parts.setup(l.index.partHead); err != nil {
		log.Printf("error setting up partitions: %+v", err)
		return errors.Wrap(err, "failed to set up partitions")
	}

	// TODO on startup we need to make sure index head is the latest id

	if err := l.loadState(); err != nil {
		log.Printf("error loading state: %+v\n", err)
		return err
	}

	log.Printf("Starting at log id %d, partition %d, offset %d", l.currID, l.index.partHead, l.written)

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

	return firstErr
}

func (l *Log) Write(b []byte) (int, error) {
	internal.Debugf(l.config, "LOG <- %s (%d)", internal.Prettybuf(bytes.Trim(b, "\n")), len(b))

	head := l.index.partHead

	written := l.written + len(b)
	madeNewPartition := false
	if written >= l.config.PartitionSize {
		fherr := l.parts.create(l.index.partHead)
		if fherr != nil {
			return 0, fherr
		}
		l.index.partHead = l.parts.currHead

		if err := l.parts.add(l.parts.currHead); err != nil {
			return 0, err
		}

		partlen := 1 + (l.index.partHead - l.index.partTail)

		maxParts := l.config.MaxPartitions
		if maxParts > 0 && partlen > uint64(maxParts) {
			tailPart := l.index.partTail + 1
			msg, merr := l.parts.firstMessage(tailPart)
			if merr != nil && merr != io.EOF {
				return 0, merr
			}

			if msg != nil && msg.ID > 0 {
				l.index.tail = msg.ID
			}

			// extraStart := l.index.partTail - (partlen - uint64(maxParts))
			extraStart := l.index.partTail
			n := 1
			if partlen > uint64(maxParts) {
				n = int(partlen - uint64(maxParts))
				if n > 1 {
					extraStart -= uint64(n - 1)
				}
			}

			l.index.partTail = tailPart

			// TODO move the file synchronously first, then any hook acts on the
			// moved file
			go l.parts.remove(extraStart, n)
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
		head := l.index.partHead
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
			head := l.index.partHead

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
	// if _, err := l.index.loadFromReader(); err != nil {
	// 	return 0, 0, errors.Wrap(err, "failed to initialize index")
	// }

	partn, offset := l.index.Get(id)
	// fmt.Println("index.Get(", id, ")", partn, offset)

	var r *internal.LogFile
	r, err := l.parts.getReader(partn)
	if err != nil {
		partn++
		r, err = l.parts.getReader(partn)
		if err != nil {
			return partn, 0, errors.Wrap(err, "failed to set read handle")
		}
		offset = 0
	}
	defer r.Close()

	if id == 1 && !inclusive {
		return 0, 0, nil
	}

	scanner := protocol.NewScanner(l.config, r)

Loop:
	for {
		scanner.Reset(r)

		for scanner.Scan() {
			msg := scanner.Message()
			// fmt.Println("scanning for id", id, scanner.Error(), msg.String())
			if msg.ID == id {
				break Loop
			}
			if msg.ID > id {
				return partn, 0, errors.New("failed to scan to id")
			}
		}

		if err := scanner.Error(); err == io.EOF {
			head := l.index.partHead
			if partn < head {
				partn++
				r.Close()
				r, err = l.parts.getReader(partn)
				if err != nil {
					return partn, 0, errors.Wrap(err, "failed to get partition")
				}

				offset = 0
				continue
			} else {
				break
			}
		} else if err != nil {
			return partn, 0, errors.Wrap(err, "failed to scan log")
		}
	}

	off := int64(offset) + int64(scanner.ChunkPos)
	if !inclusive {
		off -= int64(scanner.LastChunkPos)
	}
	// fmt.Println(offset, scanner.ChunkPos, scanner.LastChunkPos)
	internal.Debugf(l.config, "looked up id %d: partition: %d, offset: %d", id, partn, off)
	return partn, off, nil
}

// Head returns the current latest ID
func (l *Log) Head() (uint64, error) {
	return l.index.head, nil
}

// Tail returns the lowest id in the log
func (l *Log) Tail() (uint64, error) {
	// return l.index.tail, nil

	partn := l.index.partTail

	r, err := l.parts.getReader(partn)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	var msg *protocol.Message
	scanner := protocol.NewScanner(l.config, r)
	for scanner.Scan() {
		if scanned := scanner.Message(); scanned != nil {
			msg = scanned
		}
		break
	}

	err = scanner.Error()
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

	endpart, endoff, pcerr := l.getPartOffset(end, true)
	if pcerr != nil {
		return nil, errors.Wrap(pcerr, "failed to get range upper bound")
	}

	currpart, curroff, perr := l.getPartOffset(start, false)
	if perr != nil {
		return nil, errors.Wrap(perr, "failed to get range lower bound")
	}

	r, err := l.parts.getReader(currpart)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get readable logFile")
	}

	internal.Debugf(l.config, "Range: start %d:%d end %d:%d", currpart, curroff, endpart, endoff)

	var lf LogReadableFile
	fn := func() (LogReadableFile, error) {
		if lf != nil {
			currpart++
			l.parts.currReadPart = currpart
			head := l.index.partHead
			if currpart > endpart || currpart > head {
				return nil, io.EOF
			}
			curroff = 0

			r, err = l.parts.getReader(currpart)
			if err != nil {
				return nil, errors.Wrap(err, "failed to get next reader")
			}
		} else {
			if _, serr := r.Seek(curroff, io.SeekStart); serr != nil {
				return nil, errors.Wrap(serr, "failed to seek log in range query")
			}
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
