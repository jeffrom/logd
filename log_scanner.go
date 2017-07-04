package logd

import (
	"bufio"
	"io"
)

type logScanner struct {
	config *Config
	err    error
	msg    *Message
	br     *bufio.Reader
}

func newLogScanner(config *Config, r io.Reader) *logScanner {
	return &logScanner{
		config: config,
		br:     bufio.NewReader(r),
	}
}

func (s *logScanner) Scan() bool {
	msg, err := msgFromReader(s.br)
	s.err = err
	if err == io.EOF {
		s.err = nil
	}
	if err != nil {
		return false
	}

	s.msg = msg
	return true
}

func (s *logScanner) Msg() *Message {
	return s.msg
}

func (s *logScanner) Error() error {
	return s.err
}
