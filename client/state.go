package client

import (
	"fmt"
	"os"
)

// StatePusher saves recently used offsets so they can be retrieved
type StatePusher interface {
	// TODO Push should also take an error argument so it can handle failures
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

// StateOutputter writes offsets to a file. Intended for use by command line
// applications.
type StateOutputter struct {
	f *os.File
}

// NewStateOutputter returns a new oneee
func NewStateOutputter(f *os.File) *StateOutputter {
	return &StateOutputter{
		f: f,
	}
}

// Push implements StatePusher
func (m *StateOutputter) Push(off uint64) error {
	_, err := fmt.Fprintf(m.f, "%d\n", off)
	return err
}

// Close implements StatePusher
func (m *StateOutputter) Close() error {
	return nil
}

// StatePuller keeps track of the last scanned message
type StatePuller interface {
	Get() (uint64, error)
	Complete(off uint64) error
}

// FileStatePuller tracks offset state in a file
type FileStatePuller struct {
	conf *Config
	name string
	f    *os.File
}

// NewFileStatePuller returns a new instance of *FileStatePuller
func NewFileStatePuller(name string, conf *Config) *FileStatePuller {
	return &FileStatePuller{
		name: name,
		conf: conf,
	}
}

// Get implements FileStatePuller
func (m *FileStatePuller) Get() (uint64, error) {
	return 0, nil
}

// Complete implements FileStatePuller
func (m *FileStatePuller) Complete(off uint64) error {
	return nil
}
