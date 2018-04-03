package logger

// filePartitions manage file handles used in writing to the log. In order to
// write to the log, we need a writer, index read/write, and reader so we can
// figure out where we are on startup.
//
// When handling read requests, we should use file handles in a different way.

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

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
	headFresh    bool
}

func newFilePartitions(config *config.Config) *filePartitions {
	return &filePartitions{
		config: config,
	}
}

func (p *filePartitions) reset() {
	p.written = 0
	p.currReadPart = 0
	p.currHead = 0
	p.headFresh = false
}

func (p *filePartitions) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p *filePartitions) shutdown() error {
	return internal.CloseAll([]io.Closer{p.w, p.r})
}

func (p *filePartitions) setCurrentFileHandles(create bool) error {
	curr, err := p.head()
	if err != nil {
		return err
	}
	if create {
		curr++
		p.currHead = curr
	}
	if serr := p.setWriteHandle(curr); serr != nil {
		return serr
	}

	return p.setReadHandle(curr)
}

func (p *filePartitions) remove(start, end uint64) error {
	log.Printf("Deleting %d partitions: %d -> %d", start-end, start, end)
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
	return os.Remove(p.logFilePath(part))
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
	path := p.logFilePath(n)

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
	path := p.logFilePath(n)

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
	p.r = newLogFile(r)
	return nil
}

func (p *filePartitions) logFilePath(part uint64) string {
	return fmt.Sprintf("%s.%d", p.config.LogFile, part)
}

func (p *filePartitions) head() (uint64, error) {
	if p.headFresh {
		return p.currHead, nil
	}

	parts, err := p.partitions()
	if err != nil {
		return 0, nil
	}
	n := maxUint64(parts...)
	p.currHead = n
	p.headFresh = true
	return n, nil
}

func (p *filePartitions) tail() (uint64, error) {
	parts, err := p.partitions()
	if err != nil {
		return 0, nil
	}
	n := minUint64(parts...)
	return n, nil
}

func (p *filePartitions) firstMessage(part uint64) (*protocol.Message, error) {
	f, err := os.Open(p.logFilePath(part))
	if err != nil {
		return nil, errors.Wrap(err, "failed to open partition for reading")
	}
	defer f.Close()

	scanner := protocol.NewScanner(p.config, newLogFile(f))
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

func (p *filePartitions) matches() ([]string, error) {
	files, err := ioutil.ReadDir(filepath.Dir(p.config.LogFile))
	if err != nil {
		return nil, err
	}

	res := make([]string, len(files))
	n := 0
	for _, f := range files {
		dotidx := strings.LastIndex(f.Name(), ".")
		if dotidx >= 0 && strings.LastIndexAny(f.Name(), "1234567890") > dotidx {
			res[n] = f.Name()
			n++
		}
	}
	return res[:n], nil
}

func (p *filePartitions) partitions() ([]uint64, error) {
	matches, err := p.matches()
	if err != nil {
		return nil, err
	}

	res := make([]uint64, len(matches))
	for i, m := range matches {
		parts := strings.Split(m, ".")
		s := parts[len(parts)-1]

		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}

		res[i] = n
	}

	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res, nil
}
