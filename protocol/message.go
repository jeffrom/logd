package protocol

import (
	"bufio"
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/pkg/errors"
)

// Message is a new message type
type Message struct {
	conf          *config.Config
	Offset        uint64 // firstOffset + offsetDelta
	Body          []byte
	Size          int // size of the message, not including \r\n
	fullSize      int
	firstOffset   uint64 // the offset of the beginning of the batch
	offsetDelta   uint64 // the the offset of the message from firstOffset
	partition     uint64
	read          int64
	completedRead bool
	digitbuf      [32]byte
}

// NewMessage returns a Message
// MSG <size>\r\n<body>\r\n
func NewMessage(conf *config.Config) *Message {
	return &Message{
		conf: conf,
		Body: make([]byte, conf.MaxBatchSize), // TODO MaxMessageSize
	}
}

// Reset sets the message to its initial value so i can be reused
func (m *Message) Reset() {
	m.Offset = 0
	m.Size = 0
	m.fullSize = 0
	m.firstOffset = 0
	m.offsetDelta = 0
	m.partition = 0
	m.read = 0
	m.completedRead = false
}

// func (m *Message) String() string {
// 	return fmt.Sprintf("%+v", *m)
// }

// BodyBytes returns the bytes of the message body
func (m *Message) BodyBytes() []byte {
	return m.Body[:m.Size]
}

// SetBody sets the body of a message
func (m *Message) SetBody(b []byte) {
	n := copy(m.Body, b)
	m.Size = n
}

// ReadFrom implements io.ReaderFrom
func (m *Message) ReadFrom(r io.Reader) (int64, error) {
	if br, ok := r.(*bufio.Reader); ok {
		n, err := m.readFromBuf(br)
		m.read = n
		return n, err
	}
	return 0, errors.Errorf("message.ReadFrom not implemented for %T", r)
}

func (m *Message) readFromBuf(r *bufio.Reader) (int64, error) {
	var total int64
	var n uint64

	word, err := r.ReadSlice(' ')
	total += int64(len(word))
	if err != nil {
		return total, err
	}

	if !bytes.Equal(word, bmsgStart) {
		return total, errInvalidProtocolLine
	}

	word, err = r.ReadSlice('\n')
	total += int64(len(word))
	if err != nil {
		return total, err
	}
	n, err = asciiToUint(word[:len(word)-termLen])
	if err != nil {
		return total, err
	}
	m.Size = int(n)

	bodyRead, err := io.ReadFull(r, m.Body[:m.Size])
	total += int64(bodyRead)
	if err != nil {
		return total, err
	}

	newLineRead, err := readNewLine(r)
	total += int64(newLineRead)
	if err != nil {
		return total, err
	}

	m.completedRead = true
	return total, nil
}

// FromBytes populates a Message from a byte slice, returning bytes read and
// an error, if any
func (m *Message) FromBytes(b []byte) (int, error) {
	var total int
	// fmt.Printf("FromBytes(%q)\n", b)

	n, word, err := parseWordN(b)
	total += n
	if err != nil {
		return total, err
	}

	if !bytes.Equal(word, bmsg) {
		return total, errInvalidProtocolLine
	}

	n, word, err = parseWordN(b[total:])
	total += n
	if err != nil {
		return total, err
	}
	x, err := asciiToUint(word)
	if err != nil {
		return total, err
	}
	m.Size = int(x)

	// fmt.Printf("rest of slice: %q\n", b[total:])
	m.Body = b[total : total+m.Size]
	total += m.Size
	total += termLen

	// fmt.Println("parsed", m)
	return total, err
}

// WriteTo implements io.WriterTo
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var total int64

	n, err := w.Write(bmsgStart)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(m.Size), &m.digitbuf)
	n, err = w.Write(m.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.BodyBytes())
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

func (m *Message) calcSize() int {
	return MessageSize(len(m.Body))
}

// MessageSize returns the size of the message, including protocol
func MessageSize(bodySize int) int {
	l := bodySize
	l += asciiSize(l)
	l += len(bmsgStart) // `MSG `
	l += termLen * 2    // `\r\n`, both of them

	return l
}
