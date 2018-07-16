package logger

import (
	"io"
	"os"
	"path"
	"strconv"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
)

// LogWriter is the new log writer interface
type LogWriter interface {
	io.WriteCloser
	Flush() error
	SetPartition(off uint64) error
}

// Writer writes to the log
type Writer struct {
	conf  *config.Config
	f     *os.File
	topic string
}

// NewWriter returns a new instance of Writer
func NewWriter(conf *config.Config, topic string) *Writer {
	return &Writer{
		conf:  conf,
		topic: topic,
	}
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// Flush implements LogWriter interface
func (w *Writer) Flush() error {
	return w.f.Sync()
}

// SetPartition implements LogWriter interface
func (w *Writer) SetPartition(off uint64) error {
	if err := w.Close(); err != nil {
		return err
	}

	s := strconv.FormatUint(off, 10)
	p := path.Join(w.conf.WorkDir, w.topic, s+".log")
	internal.Debugf(w.conf, "opening partition %s", p)
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	w.f = f
	return err
}

// Close implements LogWriter interface
func (w *Writer) Close() error {
	if w.f != nil {
		return w.f.Close()
	}
	return nil
}

// Setup implements internal.LifecycleManager
func (w *Writer) Setup() error {
	return os.MkdirAll(path.Join(w.conf.WorkDir, w.topic), 0700)
}

// Shutdown implements LifecycleManager interface
func (w *Writer) Shutdown() error {
	return w.Close()
}
