package logd

// filePartitions manage file handles used in writing to the log. In order to
// write to the log, we need a writer, index read/write, and reader so we can
// figure out where we are on startup.
//
// When handling read requests, we should use file handles in a different way.

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

type filePartitions struct {
	config       *Config
	data         []logReadableFile
	r            logReadableFile
	w            logWriteableFile
	written      int
	currReadPart uint64
}

func newPartitions(config *Config) *filePartitions {
	return &filePartitions{
		config: config,
	}
}

func (p *filePartitions) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p *filePartitions) shutdown() error {
	var firstErr error

	if err := p.w.Close(); err != nil {
		err = errors.Wrap(err, "failed closing writeable file during shutdown")
		log.Printf("%+v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	if err := p.r.Close(); err != nil {
		err = errors.Wrap(err, "failed closing readable file during shutdown")
		log.Printf("%+v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (p *filePartitions) setCurrentFileHandles(create bool) error {
	curr := p.head()
	if create {
		curr++
	}
	if err := p.setWriteHandle(curr); err != nil {
		return err
	}
	if err := p.setReadHandle(curr); err != nil {
		return err
	}
	return nil
}

func (p *filePartitions) setHandles(n uint64) error {
	if err := p.setWriteHandle(n); err != nil {
		return err
	}
	if err := p.setReadHandle(n); err != nil {
		return err
	}
	return nil
}

func (p *filePartitions) setWriteHandle(n uint64) error {
	path := fmt.Sprintf("%s.%d", p.config.LogFile, n)

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
	if n == p.currReadPart && p.r != nil {
		return nil
	}
	path := fmt.Sprintf("%s.%d", p.config.LogFile, n)

	if p.r != nil {
		p.r.Close()
	}

	p.currReadPart = n

	r, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "failed to open log for reading")
	}
	p.r = r
	return nil
}

func (p *filePartitions) head() uint64 {
	n := maxUint64(p.partitions()...)
	return n
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

func (p *filePartitions) matches() []string {
	matches, err := filepath.Glob(p.config.LogFile + ".[0-9]")
	if err != nil {
		panic(err)
	}
	return matches
}

func (p *filePartitions) partitions() []uint64 {
	matches := p.matches()
	var res []uint64

	for _, m := range matches {
		parts := strings.Split(m, ".")
		s := parts[len(parts)-1]
		var n uint64
		fmt.Sscanf(s, "%d", &n)
		res = append(res, n)
	}

	return res
}
