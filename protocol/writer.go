package protocol

import (
	"bytes"
)

// Writer constructs all command request and response bytes.
type Writer struct {
	buf    bytes.Buffer
	numbuf [32]byte
}

// NewWriter returns a new instance of a protocol writer
func NewWriter() *Writer {
	return &Writer{}
}

// Reset resets the writer
func (w *Writer) Reset() {
	w.buf.Reset()
}

// ChunkEnvelope returns a byte representation of a chunk envelope
func (w *Writer) ChunkEnvelope(size int64) (int, []byte) {
	w.Reset()
	buf := w.buf
	buf.WriteByte('+')
	n := intToASCII(size, &w.numbuf)
	buf.Write(w.numbuf[:n])
	buf.Write([]byte("\r\n"))
	b := buf.Bytes()
	return len(b), b
}

// EOF returns a byte representation of an EOF chunk response
func (w *Writer) EOF() (int, []byte) {
	b := []byte("+EOF\r\n")
	return len(b), b
}
