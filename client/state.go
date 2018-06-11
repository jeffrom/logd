package client

import (
	"fmt"
	"io"
	"os"
)

// StatePusher saves recently used offsets so they can be retrieved
type StatePusher interface {
	io.Closer
	Push(off uint64) error
}

// NoopStatePusher discards input
type NoopStatePusher struct {
}

// Push implements StatePusher
func (m *NoopStatePusher) Push(off uint64) error {
	return nil
}

// Close implements StatePusher
func (m *NoopStatePusher) Close() error {
	return nil
}

// FileStatePusher writes offsets to a file
type FileStatePusher struct {
	f *os.File
}

// NewFileStatePusher returns a new oneee
func NewFileStatePusher(f *os.File) *FileStatePusher {
	return &FileStatePusher{
		f: f,
	}
}

// Push implements StatePusher
func (m *FileStatePusher) Push(off uint64) error {
	_, err := fmt.Fprintf(m.f, "%d\n", off)
	return err
}

// Close implements StatePusher
func (m *FileStatePusher) Close() error {
	return nil
}
