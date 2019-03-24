package logger

import (
	"io"
	"os"
	"path"
	"strconv"
)

type IndexManager interface {
	// WriteIndex(topic string, part uint64, r io.Reader) (int64, error)
	GetIndex(topic string, part uint64) (io.ReadWriteCloser, error)
	RemoveIndex(topic string, part uint64) error
}

type FileIndex struct {
	workDir string
}

func NewFileIndex(workDir string) *FileIndex {
	return &FileIndex{
		workDir: workDir,
	}
}

// func (idx *FileIndex) WriteIndex(topic string, part uint64, r io.Reader) (int64, error) {
// 	s := strconv.FormatUint(part, 10)
// 	p := path.Join(idx.workDir, topic, s+".idx")
// 	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer f.Close()

// 	return io.Copy(f, r)
// }

func (idx *FileIndex) GetIndex(topic string, part uint64) (io.ReadWriteCloser, error) {
	p := idx.path(topic, part)
	return os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0600)
}

func (idx *FileIndex) RemoveIndex(topic string, part uint64) error {
	p := idx.path(topic, part)
	return os.Remove(p)
}

func (idx *FileIndex) path(topic string, part uint64) string {
	s := strconv.FormatUint(part, 10)
	p := path.Join(idx.workDir, topic, s+".idx")
	return p
}

// type discardReadWriter struct {
// 	ioutil.Discard
// }

// type NoopQueryIndex struct{}

// func (idx *NoopQueryIndex) GetIndex(topic string, part uint64) (io.ReadWriteCloser, error) {
// 	return nil, nil
// }

// func (idx *NoopQueryIndex) RemoveIndex(topic string, part uint64) error { return nil }
