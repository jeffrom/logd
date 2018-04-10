package protocol

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"strconv"
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

// Command returns a byte representation of the command
func (w *Writer) Command(cmd *Command) (int, []byte) {
	w.Reset()
	buf := w.buf
	buf.WriteString(cmd.Name.String())
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatInt(int64(len(cmd.Args)), 10))
	buf.WriteString("\r\n")
	// buf.WriteString(fmt.Sprintf("%s %d\r\n", cmd.Name.String(), len(cmd.Args)))
	for _, arg := range cmd.Args {
		buf.WriteString(strconv.FormatInt(int64(len(arg)), 10))
		buf.WriteByte(' ')
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	b := buf.Bytes()
	return len(b), b
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

// Response returns a byte representation of a Response
func (w *Writer) Response(r *Response) (int, []byte) {
	if r.Status == RespEOF {
		return w.EOF()
	}

	w.Reset()
	buf := w.buf
	buf.WriteString(r.Status.String())

	if r.ID > 0 && r.Body != nil {
		panic("invalid response: id and body both set")
	}

	if r.ID != 0 {
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatUint(r.ID, 10))
	} else if r.Status == RespOK && r.Body != nil {
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatInt(int64(len(r.Body)), 10))
		buf.WriteByte(' ')
		buf.Write(r.Body)
	} else if r.Body != nil {
		buf.WriteByte(' ')
		buf.Write(r.Body)
	}

	buf.WriteString("\r\n")
	b := buf.Bytes()
	return len(b), b
}

// Message returns a byte representation of a message
func (w *Writer) Message(m *Message) []byte {
	w.Reset()
	checksum := crc32.Checksum(m.Body, crcTable)
	// fmt.Printf("write log: %d %d %d %q\n", m.ID, len(m.Body), checksum, m.Body)
	return []byte(fmt.Sprintf("%d %d %d %s\r\n", m.ID, len(m.Body), checksum, m.Body))
}
