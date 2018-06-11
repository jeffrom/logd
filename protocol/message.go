package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/pkg/errors"
)

// Message is a log message type.
type Message struct {
	ID        uint64
	Body      []byte
	partition uint64
	offset    uint64
	checksum  uint32
	size      int
	allread   bool
}

// NewMessage returns a new instance of a Message.
func NewMessage(id uint64, body []byte) *Message {
	// fmt.Printf("NewMessage: %d -> %q\n", id, body)
	return &Message{
		ID:   id,
		Body: body,
	}
}

// Reset resets the message data
func (m *Message) Reset() {
	m.ID = 0
	m.Body = nil
	m.partition = 0
	m.offset = 0
	m.checksum = 0
	m.size = 0
	m.allread = false
}

func (m *Message) logBytes() []byte {
	b := NewWriter().Message(m)
	return []byte(b)
}

func (m *Message) String() string {
	return string(m.logBytes())
}

// MsgFromBytes loads the message from b
func MsgFromBytes(b []byte) (*Message, error) {
	ps := NewScanner(config.DefaultConfig, bytes.NewReader(b))
	_, msg, err := ps.ReadMessage()
	return msg, err
}

// MsgFromReader loads the message from r
func MsgFromReader(r io.Reader) (*Message, error) {
	ps := NewScanner(config.DefaultConfig, r)
	_, msg, err := ps.ReadMessage()
	return msg, err
}

func msgFromLogReader(r *bufio.Reader) (int, *Message, error) {
	var err error
	var n int
	var read int

	idBytes, err := r.ReadBytes(' ')
	read += len(idBytes)
	if err == io.EOF {
		return 0, nil, err
	}
	if err != nil {
		return 0, nil, errors.Wrap(err, "failed reading id bytes")
	}
	// fmt.Printf("id: %q\n", idBytes)

	var id uint64
	id, err = asciiToUint(idBytes)
	if err != nil {
		return read, nil, errors.Wrap(err, "invalid id bytes")
	}

	lenBytes, err := r.ReadBytes(' ')
	// fmt.Printf("length: %q\n", lenBytes)
	read += len(lenBytes)
	if err != nil {
		return read, nil, errors.Wrap(err, "failed reading length bytes")
	}

	var length uint64
	length, err = asciiToUint(lenBytes)
	if err != nil {
		return read, nil, errors.Wrap(err, "invalid length bytes")
	}

	buf := make([]byte, length+2)
	n, err = r.Read(buf)
	// fmt.Printf("msg: (%d) %q\n", length, buf)
	read += n
	if err != nil {
		return read, nil, errors.Wrap(err, "failed reading body")
	}

	return read, NewMessage(id, trimNewline(buf)), nil
}

// MessageV2 is a new message type
type MessageV2 struct {
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

// NewMessageV2 returns a MessageV2
// MSG <size>\r\n<body>\r\n
func NewMessageV2(conf *config.Config) *MessageV2 {
	return &MessageV2{
		conf: conf,
		Body: make([]byte, conf.MaxBatchSize), // TODO MaxMessageSize
	}
}

func (m *MessageV2) reset() {
	m.Offset = 0
	m.partition = 0
	m.Size = 0
	m.fullSize = 0
	m.read = 0
	m.completedRead = false
}

func (m *MessageV2) String() string {
	return fmt.Sprintf("%+v", *m)
}

// SetBody sets the body of a message
func (m *MessageV2) SetBody(b []byte) {
	n := copy(m.Body, b)
	m.Size = n
}

// ReadFrom implements io.ReaderFrom
func (m *MessageV2) ReadFrom(r io.Reader) (int64, error) {
	if br, ok := r.(*bufio.Reader); ok {
		n, err := m.readFromBuf(br)
		m.read = n
		return n, err
	}
	return 0, errors.Errorf("message.ReadFrom not implemented for %T", r)
}

func (m *MessageV2) readFromBuf(r *bufio.Reader) (int64, error) {
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

// FromBytes populates a MessageV2 from a byte slice, returning bytes read and
// an error, if any
func (m *MessageV2) FromBytes(b []byte) (int, error) {
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
func (m *MessageV2) WriteTo(w io.Writer) (int64, error) {
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

	n, err = w.Write(m.Body[:m.Size])
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

func (m *MessageV2) calcSize() int {
	l := len(m.Body)
	l += asciiSize(l) + 2
	l += len(bmsgStart) // `MSG `
	l += termLen        // `\r\n`

	// fmt.Printf("calcSize: %d %q\n", l, m.Body)
	return l
}
