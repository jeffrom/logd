package logd

import (
	"errors"
	"fmt"
	"os"

	"github.com/jeffrom/logd/protocol"
)

var ErrProcessing = errors.New("message already being processed")

// StatePusher saves recently written offsets for later processing.
type StatePusher interface {
	Push(off uint64) error
}

// Backlogger saves failed batch writes so they can be attempted later.
type Backlogger interface {
	// Backlog() should return a channel with the desired backfill size. The client
	// should attempt to write failed batches to the channel, but if the channel is
	// full, it will log the error and continue.
	Backlog() chan *Backlog
}

type Backlog struct {
	Batch *protocol.Batch
	Err   error
}

type ErrorHandler interface {
	HandleError(err error)
}

// StatePuller pulls messages from the state and marks them completed or
// failed.
type StatePuller interface {
	// Get returns the oldest message offset and delta that isn't already being
	// processed.
	Get() (uint64, uint64, error)

	// Start marks a message as being processed. It should return
	// ErrProcessing if the message is already being processed.
	Start(off, delta uint64) error

	// Complete marks a message as completed or failed. Failure is written when
	// err is not nil.
	Complete(off, delta uint64, err error) error
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

type NoopBacklogger struct{}

func (bl *NoopBacklogger) Backlog() chan *Backlog { return nil }

type NoopErrorHandler struct{}

func (eh *NoopErrorHandler) HandleError(err error) {}

// func (bl *NoopBacklogger) GetBacklog() (*protocol.Batch, error) {
// 	return nil, nil
// }

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
func (m *MockStatePusher) Push(off uint64) error {
	m.offs = append(m.offs, off)
	// m.errs = append(m.errs, oerr)
	// m.batches = append(m.batches, batch)
	return m.serr
}

// SetError sets the error to be returned from calls to Push
func (m *MockStatePusher) SetError(err error) {
	m.serr = err
}

// Next returns the next offset, error, and batch, starting from the first. if
// there are no more pushed states, the last return value will be false
func (m *MockStatePusher) Next() (uint64, bool) {
	if m.n >= len(m.offs) {
		return 0, false
	}
	off := m.offs[m.n]
	m.n++
	return off, true
}

// ErrNoState should be returned by StatePullers when the state hasn't
// stored any offset information yet.
var ErrNoState = errors.New("state uninitialized")

type NoopStatePuller int

// Get implements StatePuller interface.
func (h NoopStatePuller) Get() (uint64, uint64, error) {
	return 0, 0, ErrNoState
}

// Start implements StatePuller interface.
func (h NoopStatePuller) Start(off, delta uint64) error {
	return nil
}

// Complete implements StatePuller interface.
func (h NoopStatePuller) Complete(off, delta uint64, err error) error {
	return nil
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

// Setup implements internal.LifecycleManager.
func (m *FileStatePuller) Setup() error {
	return nil
}

// Shutdown implements internal.LifecycleManager.
func (m *FileStatePuller) Shutdown() error {
	if m.f != nil {
		return m.f.Close()
	}
	return nil
}

// Get implements StatePuller interface.
func (m *FileStatePuller) Get() (uint64, uint64, error) {
	return 0, 0, nil
}

func (m *FileStatePuller) Start(off, delta uint64) error {
	return nil
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
	err         error
}

func NewMemoryStatePuller(conf *Config) *MemoryStatePuller {
	return &MemoryStatePuller{
		conf: conf,
	}
}

// Get implements StatePuller interface
func (m *MemoryStatePuller) Get() (uint64, uint64, error) {
	if !m.initialized {
		return 0, 0, ErrNoState
	}
	return m.off, m.delta, nil
}

func (m *MemoryStatePuller) Start(off, delta uint64) error {
	return nil
}

// Complete implements StatePuller interface
func (m *MemoryStatePuller) Complete(off, delta uint64, err error) error {
	m.initialized = true
	m.off = off
	m.delta = delta
	m.err = err
	return nil
}
