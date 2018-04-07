package logger

// filePartitions manage file handles used in writing to the log. In order to
// write to the log, we need a writer, index read/write, and reader so we can
// figure out where we are on startup.
//
// When handling read requests, we should use file handles in a different way.

// partitions are stored internally in an array, where position 0 holds the
// head partition, and partitions go lower, towards the logs tail partition

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

type filePartitions struct {
	config       *config.Config
	r            LogReadableFile
	w            LogWriteableFile
	written      int
	currReadPart uint64
	currHead     uint64
	parts        []*filePartition
}

func newFilePartitions(conf *config.Config) *filePartitions {
	return &filePartitions{
		config: conf,
		parts:  make([]*filePartition, conf.MaxPartitions),
	}
}

func (p *filePartitions) setup(head uint64) error {
	n := head
	i := 0
	for n >= 0 {
		if i >= p.config.MaxPartitions {
			break
		}
		// fmt.Println(p.config.MaxPartitions, i, p.parts)
		part := p.parts[i]
		if part == nil {
			p.parts[i] = newFilePartition(p.config, n)
			part = p.parts[i]
		} else {
			if err := part.close(); err != nil {
				return err
			}
		}

		if n == 0 {
			break
		}
		n--
		i++
	}
	return nil
}

func (p *filePartitions) add(n uint64) error {
	p.rotate()

	part := p.parts[0]
	if part == nil {
		p.parts[0] = newFilePartition(p.config, n)
		part = p.parts[0]
	} else if err := part.close(); err != nil {
		return err
	}
	part.reset(n)
	return nil
}

func (p *filePartitions) rotate() {
	parts := p.parts
	if len(parts) <= 1 || parts[0] == nil {
		return
	}

	for i := len(parts) - 2; i >= 0; i-- {
		parts[i], parts[i+1] = parts[i+1], parts[i]
	}
}

func (p *filePartitions) get(n uint64) (*filePartition, error) {
	// fmt.Println("filePartition.get", n)
	// fmt.Println("coolparts", p.parts)
	head := p.parts[0]
	var last *filePartition
	for i := 0; i < len(p.parts); i++ {
		if p.parts[i] == nil {
			break
		}
		last = p.parts[i]
	}
	if n < last.n || n > head.n || p.parts[head.n-n] == nil {
		return nil, protocol.ErrNotFound
	}
	return p.parts[head.n-n], nil
}

func (p *filePartitions) getReader(n uint64) (*internal.LogFile, error) {
	part, err := p.get(n)
	if err != nil {
		return nil, err
	}
	return part.getReader()
}

func (p *filePartitions) reset() {
	p.written = 0
	p.currReadPart = 0
	p.currHead = 0
}

func (p *filePartitions) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p *filePartitions) shutdown() error {
	return internal.CloseAll([]io.Closer{p.w, p.r})
}

func (p *filePartitions) setCurrentFileHandles(curr uint64) error {
	p.currHead = curr
	if serr := p.setWriteHandle(curr); serr != nil {
		return serr
	}

	return p.setReadHandle(curr)
}

func (p *filePartitions) create(head uint64) error {
	p.currHead = head + 1
	return p.setCurrentFileHandles(p.currHead)
}

func (p *filePartitions) remove(start uint64, n int) error {
	end := start + uint64(n)
	log.Printf("Deleting %d partitions: %d -> %d", n, start, end)
	for part := start; part < end; part++ {
		if err := p.removeOne(part); err != nil {
			log.Printf("failed to delete partitions: %+v", err)
			return err
		}
	}
	return nil
}

func (p *filePartitions) removeOne(part uint64) error {
	log.Printf("Deleting partition #%d", part)
	if err := p.runDeleteHook(part); err != nil {
		return err
	}
	return os.Remove(logFilePath(p.config, part))
}

func (p *filePartitions) runDeleteHook(part uint64) error {
	return nil
}

func (p *filePartitions) setHandles(n uint64) error {
	if err := p.setWriteHandle(n); err != nil {
		return err
	}
	return p.setReadHandle(n)
}

func (p *filePartitions) setWriteHandle(n uint64) error {
	path := logFilePath(p.config, n)

	if p.w != nil {
		p.w.Close()
	}
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(p.config.LogFileMode))
	if err != nil {
		return errors.Wrap(err, "failed to open log for writing")
	}
	p.w = w
	return nil
}

func (p *filePartitions) setReadHandle(n uint64) error {
	internal.Debugf(p.config, "setReadHandle(%d->%d)", p.currReadPart, n)
	if n == p.currReadPart && p.r != nil {
		return nil
	}
	path := logFilePath(p.config, n)

	if p.r != nil {
		if err := p.r.Close(); err != nil {
			log.Printf("failed to close partition #%d: %+v", p.currReadPart, err)
		}
	}

	p.currReadPart = n

	r, err := os.Open(path)
	if err != nil {
		internal.Debugf(p.config, "failed to open log for reading: %+v", err)
		return errors.Wrap(protocol.ErrNotFound, "failed to open log for reading")
	}
	p.r = internal.NewLogFile(r)
	return nil
}

func logFilePath(conf *config.Config, part uint64) string {
	return fmt.Sprintf("%s.%d", conf.LogFile, part)
}

func (p *filePartitions) firstMessage(part uint64) (*protocol.Message, error) {
	f, err := os.Open(logFilePath(p.config, part))
	if err != nil {
		return nil, errors.Wrap(err, "failed to open partition for reading")
	}
	defer f.Close()

	scanner := protocol.NewScanner(p.config, internal.NewLogFile(f))
	scanner.Scan()
	err = scanner.Error()
	if err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "failed to scan partition")
	}
	return scanner.Message(), err
}

func maxUint64(args ...uint64) uint64 {
	var highest uint64
	for _, arg := range args {
		if arg > highest {
			highest = arg
		}
	}
	return highest
}

func minUint64(args ...uint64) uint64 {
	var lowest uint64
	var checked bool

	for _, arg := range args {
		if arg < lowest {
			lowest = arg
		}
		if !checked {
			lowest = arg
			checked = true
		}

	}

	return lowest
}

var errNoFile = errors.New("file doesnt exist")

const maxPartitionSize = 0x7FFFFFFF // 2GB

type filePartition struct {
	config *config.Config
	n      uint64
	fpool  sync.Pool
	fcount int32
	gotOne bool
}

func newFilePartition(conf *config.Config, n uint64) *filePartition {
	fp := &filePartition{
		config: conf,
		n:      n,
	}
	fp.fpool = sync.Pool{
		New: func() interface{} {
			return fp.open()
		},
	}

	return fp
}

func (p *filePartition) String() string {
	return fmt.Sprintf("<%d: %d files>", p.n, p.fcount)
}

func (p *filePartition) open() *os.File {
	fname := logFilePath(p.config, p.n)
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		return nil
	}
	f, err := os.Open(fname)
	if err != nil {
		log.Printf("fopen: %+v", err)
		return nil
	}

	return f
}

func (p *filePartition) close() error {
	if p == nil {
		return nil
	}

	for p.fcount >= 0 {
		p.fpool.Get().(*os.File).Close()
		atomic.AddInt32(&p.fcount, -1)
	}
	return nil
}

func (p *filePartition) reset(n uint64) {
	p.n = n
}

func (p *filePartition) getReader() (*internal.LogFile, error) {
	f := p.fpool.Get().(*os.File)
	if p.gotOne {
		atomic.AddInt32(&p.fcount, -1)
	} else {
		p.gotOne = true
	}

	if f == nil {
		return nil, errNoFile
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}

	var closed uint32

	return internal.NewLogFile(f).WithClose(func() {
		if closed > 0 {
			return
		}
		atomic.AddUint32(&closed, 1)
		p.fpool.Put(f)
		atomic.AddInt32(&p.fcount, 1)
	}), nil
}
