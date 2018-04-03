package logger

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
)

// byte representation
// <id> <partition> <offset>
//

var errInvalidCursor = errors.New("Invalid cursor bytes")
var errEmptyIndex = errors.New("index is empty")

type fileIndexCursor struct {
	id     uint64
	part   uint64
	offset uint64
}

func newFileIndexCursor(id uint64, part uint64, offset uint64) *fileIndexCursor {
	return &fileIndexCursor{
		id:     id,
		part:   part,
		offset: offset,
	}
}

func (c *fileIndexCursor) load(b []byte) error {
	id, n := binary.Uvarint(b)
	if n <= 0 {
		return errors.Wrap(errInvalidCursor, "failed to read id")
	}
	part, n := binary.Uvarint(b[8:])
	if n <= 0 {
		return errors.Wrap(errInvalidCursor, "failed to read partition number")
	}
	offset, n := binary.Uvarint(b[16:])
	if n <= 0 {
		return errors.Wrap(errInvalidCursor, "failed to read offset")
	}

	c.id = id
	c.part = part
	c.offset = offset
	return nil
}

func (c *fileIndexCursor) String() string {
	return fmt.Sprintf("%d\t%d\t%d", c.id, c.part, c.offset)
}

const fileIndexHeaderSize = 48
const fileIndexRowSize = 24

type fileIndex struct {
	config *config.Config

	data []*fileIndexCursor

	buf *bytes.Buffer
	r   LogIndexFile
	w   LogIndexFile
	hw  LogIndexFile
	br  *bufio.Reader
	bw  *bufio.Writer

	headerBuf []byte
	head      uint64
	tail      uint64
	partHead  uint64
	partTail  uint64
}

func zeroBuffer(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func newFileIndex(conf *config.Config) *fileIndex {
	hbuf := make([]byte, fileIndexHeaderSize)
	zeroBuffer(hbuf)

	idx := &fileIndex{
		config:    conf,
		buf:       bytes.NewBuffer(hbuf),
		headerBuf: hbuf,
	}

	return idx
}

func (idx *fileIndex) setup() error {
	if err := idx.setupReadWriters(); err != nil {
		return err
	}
	if _, err := idx.loadFromReader(); err != nil {
		return err
	}

	return nil
}

func (idx *fileIndex) shutdown() error {
	if idx.bw != nil {
		if err := idx.bw.Flush(); err != nil {
			return errors.Wrap(err, "failed to flush index to disk on shutdown")
		}
	}
	return internal.CloseAll([]io.Closer{idx.r, idx.w, idx.hw})
}

func (idx *fileIndex) reset() {
	idx.br.Reset(idx.r)
	// idx.bw.Reset(idx.w)
	idx.buf.Reset()
	idx.head = 0
	idx.tail = 0
	idx.partHead = 0
	idx.partTail = 0
}

func (idx *fileIndex) setupReadWriters() error {
	idxFileName := idx.config.IndexFileName()
	w, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(idx.config.LogFileMode))
	if err != nil {
		return errors.Wrap(err, "failed to open log index for writing")
	}

	if serr := idx.seekToCursors(w); serr != nil {
		return errors.Wrap(serr, "failed to seek index past header")
	}

	hw, err := os.OpenFile(idxFileName, os.O_WRONLY, os.FileMode(idx.config.LogFileMode))
	if err != nil {
		return errors.Wrap(err, "failed to open log index for writing header")
	}

	rs, err := os.Open(idxFileName)
	if err != nil {
		return errors.Wrap(err, "failed to open log index for reading")
	}

	idx.w = w
	idx.hw = hw
	idx.r = rs

	if idx.bw == nil {
		idx.bw = bufio.NewWriter(w)
	} else {
		idx.bw.Reset(w)
	}

	if idx.br == nil {
		idx.br = bufio.NewReader(rs)
	} else {
		idx.br.Reset(rs)
	}

	return err
}

func (idx *fileIndex) loadFromReader() (int64, error) {
	if err := idx.seekToHeader(idx.r); err != nil {
		return 0, err
	}
	idx.br.Reset(idx.r)
	idx.buf.Reset()

	headn, err := idx.readHeader()
	if err != nil {
		if errors.Cause(err) == io.EOF {
			return int64(headn), nil
		}
		return int64(headn), err
	}

	n, err := idx.buf.ReadFrom(idx.br)
	n += int64(headn)
	if err != nil {
		return n, err
	}

	b := idx.buf.Bytes()
	for i := 0; i < len(b); i += fileIndexRowSize {
		c := newFileIndexCursor(0, 0, 0)
		c.load(b[i : i+fileIndexRowSize])

		idx.data = append(idx.data, c)
	}

	return n, err
}

func (idx *fileIndex) seekToHeader(f LogIndexFile) error {
	_, err := f.Seek(0, io.SeekStart)
	return err
}

func (idx *fileIndex) seekToCursors(f LogIndexFile) error {
	_, err := f.Seek(fileIndexHeaderSize, io.SeekStart)
	return err
}

func (idx *fileIndex) readHeader() (int, error) {
	n, err := io.ReadFull(idx.br, idx.headerBuf)
	if err != nil {
		return n, errors.Wrap(err, "failed to read index header")
	}

	head, _ := binary.Uvarint(idx.headerBuf)
	tail, _ := binary.Uvarint(idx.headerBuf[8:])
	partHead, _ := binary.Uvarint(idx.headerBuf[24:])
	partTail, _ := binary.Uvarint(idx.headerBuf[32:])

	idx.head = head
	idx.tail = tail
	idx.partHead = partHead
	idx.partTail = partTail

	return n, err
}

func (idx *fileIndex) writeHeader() (int, error) {
	internal.Debugf(idx.config, "writing header: head: %d, tail: %d", idx.head, idx.tail)
	zeroBuffer(idx.headerBuf)
	binary.PutUvarint(idx.headerBuf, idx.head)
	binary.PutUvarint(idx.headerBuf[8:], idx.tail)
	binary.PutUvarint(idx.headerBuf[24:], idx.partHead)
	binary.PutUvarint(idx.headerBuf[32:], idx.partTail)

	_, err := idx.hw.Seek(0, io.SeekStart)
	if err != nil {
		return 0, errors.Wrap(err, "failed to seek to index header")
	}

	n, err := idx.hw.Write(idx.headerBuf)
	if err != nil {
		return n, errors.Wrap(err, "failed to write index header")
	}

	if n != fileIndexHeaderSize {
		return n, errors.New("wrote incorrect number of bytes to index header")
	}

	// if ferr := idx.hw.Flush(); ferr != nil {
	// 	return n, errors.Wrap(ferr, "failed to flush index header to disk")
	// }
	return n, nil
}

func (idx *fileIndex) Append(id uint64, part uint64, offset uint64) (int, error) {
	buf := make([]byte, fileIndexRowSize)
	binary.PutUvarint(buf, id)
	binary.PutUvarint(buf[8:], part)
	binary.PutUvarint(buf[16:], offset)

	n, err := idx.bw.Write(buf)
	if err != nil {
		return n, errors.Wrap(err, "failed to write to index")
	}

	idx.data = append(idx.data, newFileIndexCursor(id, part, offset))

	if ferr := idx.bw.Flush(); ferr != nil {
		return n, errors.Wrap(ferr, "failed to flush index to disk on shutdown")
	}
	return n, err
}

func (idx *fileIndex) Get(id uint64) (uint64, uint64) {
	if len(idx.data) == 0 {
		return 0, 0
	}
	curr := newFileIndexCursor(0, 0, 0)
	for _, c := range idx.data {
		if c.id >= id {
			return curr.part, curr.offset
		}

		curr = c
	}

	internal.Debugf(idx.config, "index.Get(%d) -> (part %d, offset %d)", id, curr.part, curr.offset)
	return curr.part, curr.offset
}

func (idx *fileIndex) dump() {
	fmt.Printf("head: %d, tail: %d\n\n", idx.head, idx.tail)
	fmt.Printf("id\tpart\toff\n")
	fmt.Printf("--\t----\t---\n")
	if len(idx.data) == 0 {
		return
	}

	sl := idx.data
	slIdx := 5
	if len(sl) < 5 {
		slIdx = len(sl) - 1
	}
	for _, c := range sl[:slIdx] {
		fmt.Println(c)
	}

	sl = sl[slIdx:]

	if len(idx.data) > 10 {
		fmt.Println("...")
	}

	if len(sl) > 5 {
		sl = sl[len(sl)-5:]
	}

	for _, c := range sl {
		fmt.Println(c)
	}
}
