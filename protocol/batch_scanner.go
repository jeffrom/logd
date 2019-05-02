package protocol

import (
	"bufio"
	"io"

	"github.com/jeffrom/logd/config"
)

// BatchScanner can be used to scan through a reader, iterating over batches
type BatchScanner struct {
	maxSize int
	r       io.Reader
	br      *bufio.Reader
	batch   *Batch
	err     error
	scanned int
}

// NewBatchScanner returns a new instance of *BatchScanner
func NewBatchScanner(conf *config.Config, r io.Reader) *BatchScanner {
	bs := &BatchScanner{
		r:     r,
		br:    bufio.NewReaderSize(r, conf.MaxBatchSize),
		batch: NewBatch(conf),
	}
	if conf != nil {
		bs.maxSize = conf.MaxBatchSize
	}

	return bs
}

// Reset sets *BatchScanner to its initial state
func (s *BatchScanner) Reset(r io.Reader) {
	s.r = r
	// NOTE Reset actually always allocates, newreader will reuse a
	// bufio.Reader if it's larger.
	s.br.Reset(r)
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
func (s *BatchScanner) Batch() *Batch { return s.batch }

// Error returns the current error
func (s *BatchScanner) Error() error { return s.err }

// Scanned returns the number of bytes read
func (s *BatchScanner) Scanned() int { return s.scanned }

func (s *BatchScanner) MaxSize() int { return s.maxSize }
