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
	s := strconv.FormatUint(part, 10)
	p := path.Join(idx.workDir, topic, s+".idx")
	return os.Open(p)
}
