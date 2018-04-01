package protocol

import "io"

// ReadPart is returned through a channel to the connection, which will either
// read to the client or stop.
type ReadPart interface {
	Done() bool
	Reader() io.Reader
}

// PartReader is a part of a read response with an io.Reader
type PartReader struct {
	r io.Reader
}

// NewPartReader returns a new PartReader
func NewPartReader(r io.Reader) *PartReader {
	return &PartReader{r: r}
}

// Done implements ReadPart interface
func (pr *PartReader) Done() bool {
	return false
}

// Reader implements ReadPart interface
func (pr *PartReader) Reader() io.Reader {
	return pr.r
}

// PartDone is a part of a read response indicating it is complete
type PartDone struct{}

// Done implements ReadPart interface
func (pd *PartDone) Done() bool {
	return true
}

// Reader implements ReadPart interface
func (pd *PartDone) Reader() io.Reader {
	return nil
}
