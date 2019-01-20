package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/pkg/errors"
)

// MaxTopicSize represents the maximum topic length. It is 255, as that is the
// maximum directory length in linux.
const MaxTopicSize = 255

// Batch represents a collection of Messages
// BATCH <size> <topic> <checksum> <messages>\r\n<data>
// NOTE no trailing newline after the data
type Batch struct {
	conf     *config.Config
	Size     int
	Checksum uint32
	Messages int
	topic    []byte
	ntopic   int
	msgs     []*Message
	body     []byte
	digitbuf [32]byte
	msgBuf   *bytes.Buffer
	firstOff uint64
	wasRead  bool
	nread    int
}

// NewBatch returns a new instance of a batch
func NewBatch(conf *config.Config) *Batch {
	b := &Batch{
		conf:   conf,
		msgBuf: &bytes.Buffer{},
		topic:  make([]byte, MaxTopicSize),
		msgs:   make([]*Message, 1000),
	}

	return b
}

func (b *Batch) String() string {
	return fmt.Sprintf("Batch<%p Messages: %d, Size: %d, Checksum: %d>", b, b.Messages, b.Size, b.Checksum)
}

func (b *Batch) WithConfig(conf *config.Config) *Batch {
	b.conf = conf
	return b
}

// Reset puts a batch in an initial state so it can be reused
func (b *Batch) Reset() {
	b.Size = 0
	b.Checksum = 0
	b.Messages = 0
	b.ntopic = 0
	b.firstOff = 0
	b.wasRead = false
	b.msgBuf.Reset()
}

// Empty returns true if the batch contains no messages
func (b *Batch) Empty() bool {
	if b.Messages <= 0 {
		return true
	}
	return false
}

func (b *Batch) ensureBuf() {
	if b.body == nil {
		b.body = make([]byte, b.conf.MaxBatchSize)
	}
}

// SetTopic sets the topic for a batch.
func (b *Batch) SetTopic(topic []byte) {
	copy(b.topic, topic)
	b.ntopic = len(topic)
}

// Topic returns the topic for the batch.
func (b *Batch) Topic() string {
	return string(b.TopicSlice())
}

// TopicSlice returns the topic for the batch as a byte slice. The byte slice
// is not copied.
func (b *Batch) TopicSlice() []byte {
	return b.topic[:b.ntopic]
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
	b.Size = int(n)

	b.SetTopic(req.args[1])

	n, err = asciiToUint(req.args[2])
	if err != nil {
		return b, err
	}
	b.Checksum = uint32(n)

	n, err = asciiToUint(req.args[3])
	if err != nil {
		return b, err
	}
	b.Messages = int(n)

	if len(req.body) < req.bodysize {
		return nil, errors.New("request body too small")
	}
	b.body = req.body[:req.bodysize]

	b.firstOff = uint64(len(req.envelope) + termLen)
	return b, b.Validate()
}

// Validate checks the batch's checksum
// TODO should add config.MaxMessageSize and config.MaxMessagesPerBatch and
// check them here, maybe?
func (b *Batch) Validate() error {
	// if size > MaxBatchSize || crc doesn't match
	if b.Size > b.conf.MaxBatchSize {
		return errors.New("batch too large")
	}
	if b.Checksum != b.calculateChecksum() {
		return errCrcMismatch
	}
	return nil
}

// Bytes returns a slice of raw bytes. Used by EventQ to write directly to the
// log.
func (b *Batch) Bytes() []byte {
	if b.body == nil {
		return nil
	}
	return b.body[:b.Size]
}

// Append adds a new message's bytes to the batch
func (b *Batch) Append(p []byte) error {
	if b.Messages > len(b.msgs)-1 {
		msgs := make([]*Message, len(b.msgs)*2)
		copy(msgs, b.msgs)
		b.msgs = msgs
	}
	if b.msgs[b.Messages] == nil {
		b.msgs[b.Messages] = NewMessage(b.conf)
	}
	msg := b.msgs[b.Messages]
	msg.Reset()
	msg.Body = p
	msg.Size = len(p)

	b.Messages++
	b.Size += msg.calcSize()
	return nil
}

// AppendMessage adds a new message to the batch
func (b *Batch) AppendMessage(m *Message) error {
	b.msgs[b.Messages] = m
	b.Messages++
	return nil
}

// MessageBytes returns a byte slice of the batch of messages.
func (b *Batch) MessageBytes() []byte {
	return b.body[:b.Size]
}

// SetChecksum sets the batch's crc32
func (b *Batch) SetChecksum() {
	b.Checksum = b.calculateChecksum()
}

// CalcSize calculates the full byte size of the batch. this is intended to be
// called to make sure the batch isn't larger than config.MaxBatchSize, so
// we're assuming the crc is the longest possible uint32 for now to save
// calculating the crc for every message
func (b *Batch) CalcSize() int {
	l := len(bbatchStart)      // `BATCH `
	l += asciiSize(b.Size)     // <size>
	l += len(bspace)           // ` `
	l += b.ntopic              // `<topic>`
	l += len(bspace)           // ` `
	l += maxCRCSize            // <crc>
	l += len(bspace)           // ` `
	l += asciiSize(b.Messages) // <messages>
	l += termLen               // `\r\n`
	l += b.Size                // <data>
	return l
}

func (b *Batch) calculateChecksum() uint32 {
	return crc32.Checksum(b.Bytes(), crcTable)
}

func (b *Batch) buildBodyBytes() error {
	b.msgBuf.Reset()
	for i := 0; i < b.Messages; i++ {
		m := b.msgs[i]
		n, err := m.WriteTo(b.msgBuf)
		if err != nil {
			return err
		}

		m.fullSize = int(n)
	}

	l := b.msgBuf.Len()
	b.Size = l
	b.ensureBuf()
	// fmt.Printf("buildBodyBytes: %q\n", b.msgBuf.Bytes())
	// fmt.Println(len(b.body), b.msgBuf.Len())
	if b.msgBuf.Len() > len(b.body) {
		return errTooLarge
	}
	copy(b.body[:b.Size], b.msgBuf.Bytes())

	return nil
}

// WriteTo implements io.WriterTo.
func (b *Batch) WriteTo(w io.Writer) (int64, error) {
	if !b.wasRead {
		if err := b.buildBodyBytes(); err != nil {
			return 0, err
		}

		b.SetChecksum()
	}

	var total int64
	n, err := w.Write(bbatchStart)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(b.Size), &b.digitbuf)
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

	n, err = w.Write(b.TopicSlice())
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l = uintToASCII(uint64(b.Checksum), &b.digitbuf)
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

	l = uintToASCII(uint64(b.Messages), &b.digitbuf)
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
	n, err := b.readFromBuf(r.(*bufio.Reader))
	if err != nil {
		return b.finishRead(n, err)
	}
	b.wasRead = true
	return b.finishRead(n, err)
}

// FirstOffset returns the offset delta of the first message
func (b *Batch) FirstOffset() uint64 {
	if b.firstOff == 0 {
		b.firstOff = b.calculateFirstOffset()
	}
	return b.firstOff
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

func (b *Batch) finishRead(size int64, err error) (int64, error) {
	b.nread = int(size)
	return size, err
}

// readEnvelope reads the batch protocol envelope
func (b *Batch) readEnvelope(r *bufio.Reader) (int64, error) {
	var total int64
	word, err := r.ReadSlice(' ')
	total += int64(len(word))

	// fmt.Printf("%q %+v\n", word, err)
	if len(word) == 0 {
		if err == io.EOF {
			return total, err
		}
		return total, errInvalidProtocolLine
	}
	if !bytes.Equal(word, bbatchStart) {
		return total, errors.Wrap(errInvalidProtocolLine, "batch envelope didn't start with BATCH")
	}

	if err != nil {
		return total, err
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
	b.Size = int(n)

	word, err = r.ReadSlice(' ')
	total += int64(len(word))
	if err != nil {
		return total, err
	}

	b.SetTopic(word[:len(word)-1])

	word, err = r.ReadSlice(' ')
	total += int64(len(word))
	if err != nil {
		return total, err
	}

	n, err = asciiToUint(word[:len(word)-1])
	if err != nil {
		return total, err
	}
	b.Checksum = uint32(n)

	word, err = r.ReadSlice('\n')
	total += int64(len(word))
	if err != nil {
		return total, err
	}
	n, err = asciiToUint(word[:len(word)-termLen])
	if err != nil {
		return total, err
	}
	b.Messages = int(n)
	return total, err
}

// readData reads the data portion of a batch command.
func (b *Batch) readData(r *bufio.Reader) (int64, error) {
	var total int64

	// TODO this check is redundant, should check the total batch size above this
	if b.Size > b.conf.MaxBatchSize {
		return total, errTooLarge
	}
	b.ensureBuf()
	n, err := io.ReadFull(r, b.body[:b.Size])
	total += int64(n)

	return total, err
}

func (b *Batch) calculateFirstOffset() uint64 {
	n := uint64(len(bbatchStart) +
		uintToASCII(uint64(b.Size), &b.digitbuf) +
		len(bspace) +
		uintToASCII(uint64(b.Messages), &b.digitbuf) +
		termLen +
		b.ntopic)

	return n
}

// Copy returns a newly allocated copy
func (b *Batch) Copy() *Batch {
	batch := NewBatch(b.conf)
	batch.Size = b.Size
	batch.Checksum = b.Checksum
	batch.Messages = b.Messages
	batch.SetTopic(b.TopicSlice())
	batch.body = make([]byte, len(b.body))
	copy(batch.body, b.body)
	batch.wasRead = true
	return batch
}

// FullSize returns the full size of the batch if it was previously read. The
// second return value indicates whether the batch was read or not.
func (b *Batch) FullSize() (int, bool) {
	if b.wasRead {
		return b.nread, true
	}
	return 0, false
}
