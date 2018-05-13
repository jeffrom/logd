package protocol

import (
	"bufio"
	"bytes"
	"hash/crc32"
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
	Offset        uint64
	Body          []byte
	conf          *config.Config
	partition     uint64
	checksum      uint32 // crc of Body
	Size          int    // size of the message, not including \r\n
	read          int64
	completedRead bool
	digitbuf      [32]byte
}

// NewMessageV2 returns a MessageV2
// MSG <size> <crc>\r\n<body>\r\n
func NewMessageV2(conf *config.Config) *MessageV2 {
	return &MessageV2{
		conf: conf,
		Body: make([]byte, conf.MaxChunkSize),
	}
}

func (m *MessageV2) reset() {
	m.Offset = 0
	m.partition = 0
	m.checksum = 0
	m.Size = 0
	m.read = 0
	m.completedRead = false
}

// SetBody sets the body of a message
func (m *MessageV2) SetBody(b []byte) {
	n := copy(m.Body, b)
	m.Size = n
	m.setChecksum()
}

func (m *MessageV2) setChecksum() {
	m.checksum = crc32.Checksum(m.Body[:m.Size], crcTable)
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

	word, err = r.ReadSlice(' ')
	total += int64(len(word))
	if err != nil {
		return total, err
	}
	n, err = asciiToUint(word[:len(word)-1])
	if err != nil {
		return total, err
	}
	m.Size = int(n)

	word, err = r.ReadSlice('\n')
	total += int64(len(word))
	if err != nil {
		return total, err
	}
	n, err = asciiToUint(word[:len(word)-termLen])
	if err != nil {
		return total, err
	}
	m.checksum = uint32(n)

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

	n, err = w.Write(bspace)
	total += int64(n)
	if err != nil {
		return total, err
	}

	l = uintToASCII(uint64(m.checksum), &m.digitbuf)
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
