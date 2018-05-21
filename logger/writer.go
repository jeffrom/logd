package logger

import (
	"io"
	"os"
	"strconv"

	"github.com/jeffrom/logd/config"
)

// LogWriterV2 is the new log writer interface
type LogWriterV2 interface {
	io.Writer
	Flush() error
	SetPartition(off uint64) error
}

// Writer writes to the log
type Writer struct {
	conf *config.Config
	f    *os.File
}

// NewWriter returns a new instance of Writer
func NewWriter(conf *config.Config) *Writer {
	return &Writer{
		conf: conf,
	}
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// Flush implements LogWriterV2 interface
func (w *Writer) Flush() error {
	return w.f.Sync()
}

// SetPartition implements LogWriterV2 interface
func (w *Writer) SetPartition(off uint64) error {
	if w.f != nil {
		if err := w.f.Close(); err != nil {
			return err
		}
	}
	s := strconv.FormatUint(off, 10)
	f, err := os.OpenFile(w.conf.LogFile+s+".log", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	w.f = f
	return err
}
