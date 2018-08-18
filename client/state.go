package client

import (
	"errors"
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

// errNoState should be returned by StatePullers when the state hasn't
// stored any offset information yet.
var errNoState = errors.New("state uninitialized")

// StatePuller keeps track of the last scanned message
type StatePuller interface {
	Get() (uint64, uint64, error)
	Complete(off, delta uint64) error
}

// FileStatePuller tracks offset state in a file.
type FileStatePuller struct {
	conf *Config
	name string
	f    *os.File
}

// NewFileStatePuller returns a new instance of *FileStatePuller.
func NewFileStatePuller(conf *Config, name string) *FileStatePuller {
	return &FileStatePuller{
		name: name,
		conf: conf,
	}
}

func (m *FileStatePuller) isReady() bool {
	if m.f == nil {
		return false
	}
	return true
}

// Get implements StatePuller interface.
func (m *FileStatePuller) Get() (uint64, uint64, error) {
	return 0, 0, nil
}

// Complete implements StatePuller interface.
func (m *FileStatePuller) Complete(off, delta uint64) error {
	return nil
}

type MemoryStatePuller struct {
	conf        *Config
	off         uint64
	delta       uint64
	initialized bool
}

func NewMemoryStatePuller(conf *Config) *MemoryStatePuller {
	return &MemoryStatePuller{
		conf: conf,
	}
}

// Get implements StatePuller interface
func (m *MemoryStatePuller) Get() (uint64, uint64, error) {
	if !m.initialized {
		return 0, 0, errNoState
	}
	return m.off, m.delta, nil
}

// Complete implements StatePuller interface
func (m *MemoryStatePuller) Complete(off, delta uint64) error {
	m.initialized = true
	m.off = off
	m.delta = delta
	return nil
}
