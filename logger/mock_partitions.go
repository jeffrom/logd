package logger

import (
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
)

const mockCallLimit = 50

// CreateCall contains the arguments passed to a call to Create
type CreateCall struct {
	offset uint64
}

// RemoveCall contains the arguments passed to a call to Remove
type RemoveCall struct {
	offset uint64
}

// GetCall contains the arguments passed to a call to Get
type GetCall struct {
	offset uint64
	delta  int64
	limit  int64
}

// MockPartitions can be used for testing purposes
type MockPartitions struct {
	conf       *config.Config
	partitions []*MockPartition
	idsBuf     []uint64
	nparts     int

	nextErr         error
	createCalls     []CreateCall
	ncreateCalls    int
	failCreateAfter int
	removeCalls     []RemoveCall
	nremoveCalls    int
	failRemoveAfter int
	getCalls        []GetCall
	ngetCalls       int
	failGetAfter    int
	nlistCalls      int
	failListAfter   int
}

// NewMockPartitions returns an instance of MockPartitions
func NewMockPartitions(conf *config.Config) *MockPartitions {
	return &MockPartitions{
		conf:        conf,
		partitions:  make([]*MockPartition, conf.MaxPartitions),
		idsBuf:      make([]uint64, conf.MaxPartitions),
		createCalls: make([]CreateCall, mockCallLimit),
		removeCalls: make([]RemoveCall, mockCallLimit),
		getCalls:    make([]GetCall, mockCallLimit),
	}
}

func (m *MockPartitions) reset() {
	m.nextErr = nil

	m.ncreateCalls = -1
	m.nremoveCalls = -1
	m.ngetCalls = -1
	m.nlistCalls = -1
}

// Create implements PartitionManager
func (m *MockPartitions) Create(offset uint64) (Partitioner, error) {
	if m.ncreateCalls < 0 {
		m.ncreateCalls = 0
	}
	m.createCalls[m.ncreateCalls] = CreateCall{offset}
	m.ncreateCalls++
	if m.nextErr != nil && m.ncreateCalls > m.failCreateAfter {
		return nil, m.nextErr
	}

	p := NewMockPartition(m.conf)
	m.partitions[m.nparts] = p
	m.nparts++
	return p, nil
}

// Remove implements PartitionManager
func (m *MockPartitions) Remove(offset uint64) error {
	if m.nremoveCalls < 0 {
		m.nremoveCalls = 0
	}
	m.removeCalls[m.nremoveCalls] = RemoveCall{offset}
	m.nremoveCalls++
	if m.nextErr != nil && m.nremoveCalls > m.failRemoveAfter {
		return m.nextErr
	}
	return nil
}

// Get implements PartitionManager
func (m *MockPartitions) Get(offset uint64, delta int64, limit int64) (io.Reader, error) {
	if m.ngetCalls < 0 {
		m.ngetCalls = 0
	}
	m.getCalls[m.ngetCalls] = GetCall{offset, delta, limit}
	m.ngetCalls++
	if m.nextErr != nil && m.ngetCalls > m.failGetAfter {
		return nil, m.nextErr
	}

	for i := 0; i < m.nparts; i++ {
		p := m.partitions[i]
		if p.Offset() == offset {
			return p.Reader(), nil
		}
	}
	return nil, ErrNotFound
}

// List implements PartitionManager
func (m *MockPartitions) List() ([]uint64, error) {
	if m.nlistCalls < 0 {
		m.nlistCalls = 0
	}
	m.nlistCalls++
	if m.nextErr != nil && m.nlistCalls > m.failListAfter {
		return nil, m.nextErr
	}

	for i := 0; i < m.nparts; i++ {
		m.idsBuf[i] = m.partitions[i].Offset()
	}
	return m.idsBuf[:m.nparts], nil
}

// SetNextError sets an error to be returned from the next call
func (m *MockPartitions) SetNextError(err error) *MockPartitions {
	m.nextErr = err
	return m
}

// FailCreateAfter returns an error after Create is called n times
func (m *MockPartitions) FailCreateAfter(n int) *MockPartitions {
	m.failCreateAfter = n
	return m
}

// FailRemoveAfter returns an error after Remove is called n times
func (m *MockPartitions) FailRemoveAfter(n int) *MockPartitions {
	m.failRemoveAfter = n
	return m
}

// FailGetAfter returns an error after Get is called n times
func (m *MockPartitions) FailGetAfter(n int) *MockPartitions {
	m.failGetAfter = n
	return m
}

// FailListAfter returns an error after List is called n times
func (m *MockPartitions) FailListAfter(n int) *MockPartitions {
	m.failListAfter = n
	return m
}

// CreateCalls returns a list containing the arguments supplied to each previous Create call
func (m *MockPartitions) CreateCalls() []CreateCall {
	return m.createCalls
}

// RemoveCalls returns a list containing the arguments supplied to each previous Remove call
func (m *MockPartitions) RemoveCalls() []RemoveCall {
	return m.removeCalls
}

// GetCalls returns a list containing the arguments supplied to each previous Get call
func (m *MockPartitions) GetCalls() []GetCall {
	return m.getCalls
}

// ListCalls returns the number of times List was called
func (m *MockPartitions) ListCalls() int {
	return m.nlistCalls
}

// MockPartition can be used for testing purposes
type MockPartition struct {
	conf   *config.Config
	offset uint64
	data   []byte
	reader io.ReadCloser
	buf    *closingBuffer
}

type closingBuffer struct {
	*bytes.Buffer
}

func newClosingBuffer() *closingBuffer {
	return &closingBuffer{
		Buffer: &bytes.Buffer{},
	}
}

func (b *closingBuffer) Close() error {
	return nil
}

// NewMockPartition returns a new instance of MockPartition
func NewMockPartition(conf *config.Config) *MockPartition {
	return &MockPartition{
		conf: conf,
		buf:  newClosingBuffer(),
	}
}

// Offset implements Partitioner
func (p *MockPartition) Offset() uint64 {
	return p.offset
}

// Reader implements Partitioner
func (p *MockPartition) Reader() io.ReadCloser {
	if p.reader == nil && p.data != nil {
		p.buf.Reset()
		p.buf.Write(p.data)
		return p.buf
	}
	return p.reader
}

// Close implements Partitioner
func (p *MockPartition) Close() error {
	return nil
}

// Size implements Partitioner
func (p *MockPartition) Size() int {
	return 0
}

// SetOffset sets the offset on the partition
func (p *MockPartition) SetOffset(off uint64) *MockPartition {
	p.offset = off
	return p
}

// SetData sets the data on the partition
func (p *MockPartition) SetData(b []byte) *MockPartition {
	p.data = b
	return p
}

// SetReader sets the reader the partition will return from Get
func (p *MockPartition) SetReader(r io.ReadCloser) *MockPartition {
	p.reader = r
	return p
}
