package logger

// filePartitions manage file handles used in writing to the log. In order to
// write to the log, we need a writer, index read/write, and reader so we can
// figure out where we are on startup.
//
// When handling read requests, we should use file handles in a different way.

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

type filePartitions struct {
	config       *config.Config
	data         []LogReadableFile
	r            LogReadableFile
	w            LogWriteableFile
	written      int
	currReadPart uint64
}

func newFilePartitions(config *config.Config) *filePartitions {
	return &filePartitions{
		config: config,
	}
}

func (p *filePartitions) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p *filePartitions) shutdown() error {
	// var firstErr error

	// if err := p.w.Close(); err != nil {
	// 	err = errors.Wrap(err, "failed closing writeable file during shutdown")
	// 	log.Printf("%+v", err)
	// 	if firstErr == nil {
	// 		firstErr = err
	// 	}
	// }

	// if err := p.r.Close(); err != nil {
	// 	err = errors.Wrap(err, "failed closing readable file during shutdown")
	// 	log.Printf("%+v", err)
	// 	if firstErr == nil {
	// 		firstErr = err
	// 	}
	// }

	return internal.CloseAll([]io.Closer{p.w, p.r})
}

func (p *filePartitions) setCurrentFileHandles(create bool) error {
	curr, err := p.head()
	if err != nil {
		return err
	}
	if create {
		curr++
	}
	if err := p.setWriteHandle(curr); err != nil {
		return err
	}

	parts, err := p.partitions()
	if err != nil {
		return err
	}
	maxParts := p.config.MaxPartitions
	if maxParts > 0 && len(parts) > maxParts {

		go func() {
			if derr := p.delete(parts[:len(parts)-maxParts]); derr != nil {
				log.Printf("failed to delete partitions: %+v", derr)
			}
		}()
	}

	return p.setReadHandle(curr)
}

func (p *filePartitions) delete(parts []uint64) error {
	log.Printf("Deleting %d partitions: %v", len(parts), parts)
	for _, part := range parts {
		if err := p.deleteOne(part); err != nil {
			return err
		}
	}
	return nil
}

func (p *filePartitions) deleteOne(part uint64) error {
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
	parts, err := p.partitions()
	if err != nil {
		return 0, nil
	}
	n := maxUint64(parts...)
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
	return filepath.Glob(p.config.LogFile + ".[0-9]*")
}

func (p *filePartitions) partitions() ([]uint64, error) {
	matches, err := p.matches()
	if err != nil {
		return nil, err
	}
	var res []uint64

	for _, m := range matches {
		parts := strings.Split(m, ".")
		s := parts[len(parts)-1]
		var n uint64
		fmt.Sscanf(s, "%d", &n)
		res = append(res, n)
	}

	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res, nil
}
