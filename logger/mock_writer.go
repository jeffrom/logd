package logger

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/jeffrom/logd/config"
)

// MockWriter can be used for testing purposes
type MockWriter struct {
	conf   *config.Config
	w      io.Writer
	parts  []*mockPartition
	nparts int
}

// NewMockWriter returns an instance of MockWriter
func NewMockWriter(conf *config.Config) *MockWriter {
	w := &MockWriter{
		conf:  conf,
		parts: make([]*mockPartition, conf.MaxPartitions),
	}

	w.setup()
	return w
}

// NewDiscardWriter returns a logger that discards all input
func NewDiscardWriter(conf *config.Config) *MockWriter {
	w := &MockWriter{
		conf:  conf,
		w:     ioutil.Discard,
		parts: make([]*mockPartition, conf.MaxPartitions),
	}
	w.setup()
	return w
}

func (w *MockWriter) setup() {
	for i := 0; i < w.conf.MaxPartitions; i++ {
		w.parts[i] = newMockPartition(w.conf)
	}
}

func (w *MockWriter) reset() {
	w.nparts = 0
}

func (w *MockWriter) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

// Flush implements LogWriter
func (w *MockWriter) Flush() error {
	if fl, ok := w.w.(interface{ Flush() error }); ok {
		return fl.Flush()
	}
	return nil
}

// SetPartition implements LogWriter
func (w *MockWriter) SetPartition(off uint64) error {
	if w.nparts == w.conf.MaxPartitions-1 {
		w.rotate()
	}

	p := w.parts[w.nparts]

	w.parts[w.nparts] = p
	if w.w != ioutil.Discard {
		w.w = p
	}

	if w.nparts < w.conf.MaxPartitions-1 {
		w.nparts++
	}
	return nil
}

// Close implements LogWriter
func (w *MockWriter) Close() error {
	return nil
}

func (w *MockWriter) rotate() {
	parts := w.parts
	if len(parts) <= 1 {
		return
	}

	for i := 0; i < len(parts)-1; i++ {
		parts[i], parts[i+1] = parts[i+1], parts[i]
	}
}

// Partitions returns a list of *bytes.Buffer-s containing the state of the log
func (w *MockWriter) Partitions() []*bytes.Buffer {
	parts := make([]*bytes.Buffer, w.nparts)

	for i := 0; i < w.nparts; i++ {
		parts[i] = w.parts[i].Buffer
	}

	return parts
}

// SetWriter sets the io.Writer on the MockWriter. The writer can also
// implement Flush.
func (w *MockWriter) SetWriter(wr io.Writer) *MockWriter {
	w.w = wr
	return w
}

type mockPartition struct {
	*bytes.Buffer
	conf        *config.Config
	startOffset uint64
	size        int
}

func newMockPartition(conf *config.Config) *mockPartition {
	return &mockPartition{
		conf:   conf,
		Buffer: &bytes.Buffer{},
	}
}

func (p *mockPartition) reset() {
	p.startOffset = 0
	p.size = 0
	p.Buffer.Reset()
}
