package logd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
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

type fileIndex struct {
	config *Config

	data []*fileIndexCursor

	buf *bytes.Buffer
	br  *bufio.Reader
	bw  *bufio.Writer
}

func newFileIndex(config *Config, w io.Writer, r io.Reader) *fileIndex {
	idx := &fileIndex{
		config: config,
		buf:    &bytes.Buffer{},
		br:     bufio.NewReader(r),
		bw:     bufio.NewWriter(w),
	}

	return idx
}

func (idx *fileIndex) shutdown() error {
	if err := idx.bw.Flush(); err != nil {
		return errors.Wrap(err, "failed to flush index to disk on shutdown")
	}
	return nil
}

func (idx *fileIndex) Append(id uint64, part uint64, offset uint64) (int, error) {
	buf := make([]byte, 24)
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

	return curr.part, curr.offset
}

func (idx *fileIndex) loadFromReader() (int64, error) {
	n, err := idx.buf.ReadFrom(idx.br)
	if err != nil {
		return n, err
	}

	b := idx.buf.Bytes()
	for i := 0; i < len(b); i += 24 {
		c := newFileIndexCursor(0, 0, 0)
		c.load(b[i : i+24])

		idx.data = append(idx.data, c)
	}

	return n, err
}

func (idx *fileIndex) dump() {
	fmt.Printf("id\tpart\toff\n")
	fmt.Printf("--\t----\t---\n")
	if len(idx.data) == 0 {
		return
	}

	sl := idx.data
	if len(idx.data) > 10 {
		fmt.Println("...")
		sl = idx.data[len(idx.data)-10:]
	}

	for _, c := range sl {
		fmt.Println(c)
	}
}
