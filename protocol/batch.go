package protocol

import (
	"bufio"
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
)

// Batch represents a collection of Messages
// BATCH <size> <messages>\r\n<data>\r\n
type Batch struct {
	config   *config.Config
	buf      []byte
	size     uint64
	messages int
	// firstOffset uint64
	digitbuf [32]byte
	msgBuf   *bytes.Buffer
}

// NewBatch returns a new instance of a batch
func NewBatch(conf *config.Config) *Batch {
	return &Batch{
		config: conf,
		msgBuf: &bytes.Buffer{},
	}
}

// Reset puts a batch in an initial state so it can be reused
func (b *Batch) Reset() {
	b.size = 0
	b.messages = 0
}

func (b *Batch) ensureBuf() {
	if b.buf == nil {
		b.buf = make([]byte, b.config.MaxChunkSize)
	}
}

// FromRequest parses a request, populating the batch. If validation fails, an
// error is returned.
func (b *Batch) FromRequest(req *Request) (*Batch, error) {
	if req.nargs != argLens[CmdBatch] {
		return b, errInvalidNumArgs
	}

	// fmt.Printf("%s(%q, %q)\n", &req.Name, req.args[0], req.args[1])
	n, err := asciiToUint(req.args[0])
	if err != nil {
		return b, err
	}
	b.size = n

	n, err = asciiToUint(req.args[1])
	if err != nil {
		return b, err
	}
	b.messages = int(n)

	b.buf = req.body[:req.bodysize]
	return b, nil
}

// Validate checks the batch's checksum
// TODO checksum
func (b *Batch) Validate() error {
	return nil
}

// Bytes returns a slice of raw bytes. Used by EventQ to write directly to the
// log.
func (b *Batch) Bytes() []byte {
	if b.buf == nil {
		return nil
	}
	return b.buf[:b.size]
}

// AppendMessage adds a new messages bytes to the batch
func (b *Batch) AppendMessage(m *MessageV2) error {
	b.msgBuf.Reset()
	_, err := m.WriteTo(b.msgBuf)
	if err != nil {
		return err
	}

	b.ensureBuf()
	n := copy(b.buf[b.size:], b.msgBuf.Bytes())
	b.size += uint64(n)
	b.messages++
	return nil
}

// MessageBytes returns a byte slice of the batch of messages.
func (b *Batch) MessageBytes() []byte {
	b.ensureBuf()
	return b.buf[:b.size]
}

// WriteTo implements io.WriterTo.
func (b *Batch) WriteTo(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(bbatchStart)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(b.size), &b.digitbuf)
	n, err = w.Write(b.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l = uintToASCII(uint64(b.messages), &b.digitbuf)
	n, err = w.Write(b.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(b.MessageBytes())
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, err
}

// ReadFrom implements io.ReaderFrom
func (b *Batch) ReadFrom(r io.Reader) (int64, error) {
	return b.readFromBuf(r.(*bufio.Reader))
}

// readFromBuf reads a batch from a *bufio.Reader
func (b *Batch) readFromBuf(r *bufio.Reader) (int64, error) {
	var total int64
	total, err := b.readEnvelope(r)
	if err != nil {
		return total, err
	}

	n, err := b.readData(r)
	total += n
	return total, err
}

// readEnvelope reads the batch protocol envelope
func (b *Batch) readEnvelope(r *bufio.Reader) (int64, error) {
	var total int64
	word, err := r.ReadSlice(' ')
	total += int64(len(word))
	if err != nil {
		return total, err
	}

	if !bytes.Equal(word, bbatchStart) {
		return total, errInvalidProtocolLine
	}

	word, err = r.ReadSlice(' ')
	total += int64(len(word))
	if err != nil {
		return total, err
	}

	var n uint64
	n, err = asciiToUint(word[:len(word)-1])
	if err != nil {
		return total, err
	}
	b.size = n

	word, err = r.ReadSlice('\n')
	total += int64(len(word))
	if err != nil {
		return total, err
	}
	n, err = asciiToUint(word[:len(word)-termLen])
	if err != nil {
		return total, err
	}
	b.messages = int(n)
	return total, err
}

// readData reads the data portion of a batch command.
func (b *Batch) readData(r *bufio.Reader) (int64, error) {
	var total int64

	b.ensureBuf()
	n, err := io.ReadFull(r, b.buf[:b.size])
	total += int64(n)
	return total, err
}

// BatchResponse is used to build a ResponseV2 io.Reader
type BatchResponse struct {
	conf      *config.Config
	partition int64
	offset    int64
	digitbuf  [32]byte
}

// NewBatchResponse returns a new instance of BatchResponse
// OK <partition> <offset>\r\n
func NewBatchResponse(conf *config.Config) *BatchResponse {
	br := &BatchResponse{conf: conf}
	br.reset()

	return br
}

func (br *BatchResponse) reset() {
	br.partition = -1
	br.offset = -1
}

// SetPartition sets the partition number for a batch response
func (br *BatchResponse) SetPartition(part int64) {
	br.partition = part
}

// SetOffset sets the offset number for a batch response
func (br *BatchResponse) SetOffset(offset int64) {
	br.offset = offset
}

// WriteTo implements io.WriterTo
func (br *BatchResponse) WriteTo(w io.Writer) (int64, error) {
	if br.partition < 0 || br.offset < 0 {
		panic("partition / offset not set when writing batch response")
	}

	var total int64
	n, err := w.Write(bok)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(br.partition), &br.digitbuf)
	n, err = w.Write(br.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l = uintToASCII(uint64(br.offset), &br.digitbuf)
	n, err = w.Write(br.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}
