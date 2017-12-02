package logd

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
)

type memLogger struct {
	messages      [][]byte
	headC         chan []byte
	returnErr     bool
	headReturnErr error
	discard       bool
	currID        uint64
}

func newMemLogger() *memLogger {
	return &memLogger{headC: make(chan []byte, 1024)}
}

func (l *memLogger) Write(b []byte) (int, error) {
	if l.returnErr {
		return 0, errors.New("hey it's an error")
	}
	if !l.discard {
		cp := make([]byte, len(b))
		copy(cp, b)
		l.messages = append(l.messages, cp)
	}

	return len(b), nil
}

func (l *memLogger) SetID(id uint64) {
	l.currID = id
}

func (l *memLogger) Flush() error {
	if l.returnErr {
		return errors.New("hey it's an error")
	}
	return nil
}

func (l *memLogger) Read(b []byte) (int, error) {
	var out []byte
	id := l.currID
	currID := id
	if id == 0 {
		id = 1
	}
	for i := id - 1; i < uint64(len(l.messages)); i++ {
		msgBytes := make([]byte, len(l.messages[i]))
		copy(msgBytes, l.messages[i])
		out = append(out, msgBytes...)
		currID++
	}
	l.currID = currID

	for i, ch := range out {
		b[i] = ch
	}
	if len(out) == 0 {
		return 0, io.EOF
	}
	return len(out), nil
}

func (l *memLogger) SeekToID(id uint64) error {
	if l.returnErr {
		return errors.New("hey it's an error")
	}
	l.currID = id
	return nil
}

func (l *memLogger) Head() (uint64, error) {
	if l.headReturnErr != nil {
		return 0, l.headReturnErr
	}

	return uint64(len(l.messages)), nil
}

func (l *memLogger) Copy() Logger {
	ml := newMemLogger()
	ml.messages = l.messages
	return ml
}

func (l *memLogger) Range(start, end uint64) (logRangeIterator, error) {
	var msgs [][]byte
	copy(msgs, l.messages)
	fn := func() (logReadableFile, error) {
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
	next func() (logReadableFile, error)
}

func memIteratorFunc(fn func() (logReadableFile, error)) *memIterator {
	return &memIterator{next: fn}
}

func (mi *memIterator) Next() (logReadableFile, error) {
	return mi.next()
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

func (mf *memFile) SizeLimit() (int64, int64) {
	return 0, 0
}

func (mf *memFile) Close() error {
	return nil
}

func (mf *memFile) Seek(n int64, off int) (int64, error) {
	return 0, nil
}
