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
	config        *Config
	r             logReadableFile
	w             logWriteableFile
	idxrw         logIndexFile
	written       int
	currPartition int
}

func newPartitions(config *Config) *filePartitions {
	return &filePartitions{
		config: config,
	}
}

func (p *filePartitions) Write(b []byte) (int, error) {
	written := p.written + len(b)
	if written > p.config.PartitionSize {
		if err := p.getFileHandles(true); err != nil {
			return 0, err
		}
		debugf(p.config, "Moved to partition %d", p.curr())
		p.written = 0
	}

	n, err := p.w.Write(b)
	if err != nil {
		return n, errors.Wrap(err, "failed to write to log")
	}

	p.written = written
	return n, err
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

	if err := p.idxrw.Close(); err != nil {
		err = errors.Wrap(err, "failed closing index file during shutdown")
		log.Printf("%+v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (p *filePartitions) getFileHandles(create bool) error {
	curr := p.curr()
	if create {
		curr++
	}
	if err := p.getWriteHandle(curr); err != nil {
		return err
	}
	if err := p.getIndexHandle(); err != nil {
		return err
	}
	if err := p.getReadHandle(curr); err != nil {
		return err
	}
	return nil
}

func (p *filePartitions) getWriteHandle(n int) error {
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

func (p *filePartitions) getReadHandle(n int) error {
	path := fmt.Sprintf("%s.%d", p.config.LogFile, n)

	if p.r != nil {
		p.r.Close()
	}
	r, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "failed to open log for reading")
	}
	p.r = r
	return nil
}

func (p *filePartitions) getIndexHandle() error {
	if p.idxrw != nil {
		p.idxrw.Close()
	}
	idxrw, err := os.OpenFile(p.config.LogFile+".index", os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(p.config.LogFileMode))
	if err != nil {
		return errors.Wrap(err, "failed to open log index")
	}
	p.idxrw = idxrw
	return nil
}

func (p *filePartitions) curr() int {
	n := maxInt(p.partitions()...)
	return n
}

func maxInt(args ...int) int {
	var highest int
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

func (p *filePartitions) partitions() []int {
	matches := p.matches()
	var res []int

	for _, m := range matches {
		parts := strings.Split(m, ".")
		s := parts[len(parts)-1]
		var n int
		fmt.Sscanf(s, "%d", &n)
		res = append(res, n)
	}

	return res
}
