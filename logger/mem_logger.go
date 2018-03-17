package logger

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
)

// MemLogger is used internally for testing. It mocks the default file logger.
type MemLogger struct {
	Messages      [][]byte
	HeadC         chan []byte
	ReturnErr     bool
	HeadReturnErr error
	Discard       bool
	CurrID        uint64
}

func NewMemLogger() *MemLogger {
	return &MemLogger{HeadC: make(chan []byte, 1024)}
}

func (l *MemLogger) Write(b []byte) (int, error) {
	if l.ReturnErr {
		return 0, errors.New("hey it's an error")
	}
	if !l.Discard {
		cp := make([]byte, len(b))
		copy(cp, b)
		l.Messages = append(l.Messages, cp)
	}

	return len(b), nil
}

// SetID implements logger.Logger
func (l *MemLogger) SetID(id uint64) {
	l.CurrID = id
}

// Flush implements logger.Logger
func (l *MemLogger) Flush() error {
	if l.ReturnErr {
		return errors.New("hey it's an error")
	}
	return nil
}

// Read implements logger.Logger
func (l *MemLogger) Read(b []byte) (int, error) {
	var out []byte
	id := l.CurrID
	currID := id
	if id == 0 {
		id = 1
	}
	for i := id - 1; i < uint64(len(l.Messages)); i++ {
		msgBytes := make([]byte, len(l.Messages[i]))
		copy(msgBytes, l.Messages[i])
		out = append(out, msgBytes...)
		currID++
	}
	l.CurrID = currID

	for i, ch := range out {
		b[i] = ch
	}
	if len(out) == 0 {
		return 0, io.EOF
	}
	return len(out), nil
}

// SeekToID implements logger.Logger
func (l *MemLogger) SeekToID(id uint64) error {
	if l.ReturnErr {
		return errors.New("hey it's an error")
	}
	l.CurrID = id
	return nil
}

// Head implements logger.Logger
func (l *MemLogger) Head() (uint64, error) {
	if l.HeadReturnErr != nil {
		return 0, l.HeadReturnErr
	}

	return uint64(len(l.Messages)), nil
}

// Copy implements logger.Logger
func (l *MemLogger) Copy() Logger {
	ml := NewMemLogger()
	ml.Messages = l.Messages
	return ml
}

// Range returns the log ids as files. start and end are inclusive. implements
// logger.Logger.
func (l *MemLogger) Range(start, end uint64) (LogRangeIterator, error) {
	var msgs [][]byte
	copy(msgs, l.Messages)
	fn := func() (LogReadableFile, error) {
		var msg []byte
		if len(msgs) == 0 {
			return nil, io.EOF
		}
		msg, msgs = msgs[0], msgs[1:]
		return newMemFile(msg), io.EOF
	}
	return memIteratorFunc(fn), nil
}

type memIterator struct {
	next func() (LogReadableFile, error)
	err  error
	lf   LogReadableFile
}

func memIteratorFunc(fn func() (LogReadableFile, error)) *memIterator {
	return &memIterator{next: fn}
}

func (mi *memIterator) Next() bool {
	lf, err := mi.next()
	mi.lf = lf
	mi.err = err
	return err == nil
}

func (mi *memIterator) Error() error {
	return mi.err
}

func (mi *memIterator) LogFile() LogReadableFile {
	return mi.lf
}

type memFile struct {
	*bufio.ReadWriter
	buf []byte
}

func newMemFile(b []byte) *memFile {
	return &memFile{
		buf:        b,
		ReadWriter: bufio.NewReadWriter(bufio.NewReader(bytes.NewBuffer(b)), bufio.NewWriter(bytes.NewBuffer(b))),
	}
}

func (mf *memFile) AsFile() *os.File {
	return nil
}

func (mf *memFile) SetLimit(limit int64) {

}

func (mf *memFile) SizeLimit() (int64, int64, error) {
	return 0, 0, nil
}

func (mf *memFile) Close() error {
	return nil
}

func (mf *memFile) Seek(n int64, off int) (int64, error) {
	return 0, nil
}
