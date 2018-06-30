package protocol

import (
	"bufio"
	"io"

	"github.com/jeffrom/logd/config"
)

// BatchScanner can be used to scan through a reader, iterating over batches
type BatchScanner struct {
	conf    *config.Config
	r       io.Reader
	br      *bufio.Reader
	batch   *Batch
	err     error
	scanned int
}

// NewBatchScanner returns a new instance of *BatchScanner
func NewBatchScanner(conf *config.Config, r io.Reader) *BatchScanner {
	return &BatchScanner{
		conf:  conf,
		r:     r,
		br:    bufio.NewReader(r),
		batch: NewBatch(conf),
	}
}

// Reset sets *BatchScanner to its initial state
func (s *BatchScanner) Reset(r io.Reader) {
	s.r = r
	// NOTE Reset actually always allocates, newreader will reuse a
	// bufio.Reader if it's larger.
	// s.br.Reset(r)
	s.br = bufio.NewReader(r)
	s.err = nil
	s.scanned = 0
}

// Scan iterates through the reader, stopping when a batch is read and
// populating the batch
func (s *BatchScanner) Scan() bool {
	s.batch.Reset()
	n, err := s.batch.ReadFrom(s.br)
	s.scanned += int(n)
	s.err = err
	return err == nil
}

// Batch returns the current *Batch
func (s *BatchScanner) Batch() *Batch {
	return s.batch
}

// Error returns the current error
func (s *BatchScanner) Error() error {
	return s.err
}

// Scanned returns the number of bytes read
func (s *BatchScanner) Scanned() int {
	return s.scanned
}
