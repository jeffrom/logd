package client

import (
	"fmt"
	"os"

	"github.com/jeffrom/logd/protocol"
)

// StatePusher saves recently used offsets so they can be retrieved
type StatePusher interface {
	Push(off uint64, err error, batch *protocol.Batch) error
}

// NoopStatePusher discards input
type NoopStatePusher struct {
}

// Push implements StatePusher
func (m *NoopStatePusher) Push(off uint64, err error, batch *protocol.Batch) error {
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
func (m *StateOutputter) Push(off uint64, oerr error, batch *protocol.Batch) error {
	_, err := fmt.Fprintf(m.f, "%d\n", off)
	return err
}

// Close implements StatePusher
func (m *StateOutputter) Close() error {
	return nil
}

// MockStatePusher saves pushed state so it can be read in tests
type MockStatePusher struct {
	offs    []uint64
	errs    []error
	batches []*protocol.Batch
	n       int
	serr    error
}

// NewMockStatePusher returns a new instance of MockStatePusher
func NewMockStatePusher() *MockStatePusher {
	return &MockStatePusher{
		offs:    make([]uint64, 0),
		errs:    make([]error, 0),
		batches: make([]*protocol.Batch, 0),
	}
}

// Push implements StatePusher
func (m *MockStatePusher) Push(off uint64, oerr error, batch *protocol.Batch) error {
	m.offs = append(m.offs, off)
	m.errs = append(m.errs, oerr)
	m.batches = append(m.batches, batch)
	return m.serr
}

// SetError sets the error to be returned from calls to Push
func (m *MockStatePusher) SetError(err error) {
	m.serr = err
}

// Next returns the next offset, error, and batch, starting from the first. if
// there are no more pushed states, the last return value will be false
func (m *MockStatePusher) Next() (uint64, error, *protocol.Batch, bool) {
	if m.n >= len(m.offs) {
		return 0, nil, nil, false
	}
	off, err, batch := m.offs[m.n], m.errs[m.n], m.batches[m.n]
	m.n++
	return off, err, batch, true
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
