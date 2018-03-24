package protocol

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"strconv"

	"github.com/pkg/errors"
)

// ProtocolWriter constructs all command request and response bytes.
type ProtocolWriter struct {
	buf bytes.Buffer
}

// NewProtocolWriter returns a new instance of a protocol writer
func NewProtocolWriter() *ProtocolWriter {
	return &ProtocolWriter{}
}

func (pw *ProtocolWriter) WriteCommand(cmd *Command) []byte {
	buf := pw.buf
	buf.Reset()
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
	return buf.Bytes()
}

func (pw *ProtocolWriter) writeChunkEnvelope(b []byte) []byte {
	buf := pw.buf
	buf.Reset()
	buf.WriteByte('+')
	buf.WriteString(strconv.FormatInt(int64(len(b)), 10))
	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (pw *ProtocolWriter) writeResponse(r *Response) ([]byte, error) {
	if r.Status == RespEOF {
		return []byte("+EOF\r\n"), nil
	}

	buf := pw.buf
	buf.Reset()
	buf.WriteString(r.Status.String())

	if r.ID > 0 && r.Body != nil {
		return nil, errors.New("invalid response: id and body both set")
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
	return buf.Bytes(), nil
}

func (pw *ProtocolWriter) WriteLogLine(m *Message) []byte {
	checksum := crc32.Checksum(m.Body, crcTable)
	// fmt.Printf("write log: %d %d %d %q\n", m.ID, len(m.Body), checksum, m.Body)
	return []byte(fmt.Sprintf("%d %d %d %s\r\n", m.ID, len(m.Body), checksum, m.Body))
}
