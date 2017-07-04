package logd

import (
	"bytes"

	"github.com/pkg/errors"
)

type fileLogger struct {
	config  *Config
	parts   *filePartitions
	written int
}

type flusher interface {
	Flush() error
}

func newFileLogger(config *Config) *fileLogger {
	return &fileLogger{
		config: config,
		parts:  newPartitions(config),
	}
}

func (l *fileLogger) Setup() error {
	if l.config.LogFile != "" {
		debugf(l.config, "Starting at partition %d", l.parts.curr())
		err := l.parts.getFileHandles(false)
		if err != nil {
			return errors.Wrap(err, "failed to get file handles")
		}
	}
	return nil
}

func (l *fileLogger) Shutdown() error {
	return l.parts.shutdown()
}

func (l *fileLogger) Write(b []byte) (int, error) {
	debugf(l.config, "LOG <- %s", bytes.Trim(b, "\n"))
	return l.parts.Write(b)
}

func (l *fileLogger) Flush() error {
	if fl, ok := l.parts.w.(flusher); ok {
		return fl.Flush()
	}
	return nil
}

func (l *fileLogger) Read(b []byte) (int, error) {
	return l.parts.r.Read(b)
}

func (l *fileLogger) SeekToID(id uint64) error {
	if _, err := l.parts.r.Seek(0, 0); err != nil {
		return err
	}

	scanner := newLogScanner(l.config, l.parts.r)
	for scanner.Scan() {
		msg := scanner.Msg()
		if msg.id == id-1 {
			return scanner.Error()
		}
	}
	return scanner.Error()
}

func (l *fileLogger) Head() (uint64, error) {
	if _, err := l.parts.r.Seek(0, 0); err != nil {
		return 0, err
	}

	scanner := newLogScanner(l.config, l.parts.r)
	for scanner.Scan() {

	}
	msg := scanner.Msg()
	if msg == nil {
		return 0, scanner.Error()
	}
	return msg.id, scanner.Error()
}
