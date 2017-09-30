package logd

import (
	"bufio"
	"io"
)

type fileLogScanner struct {
	config   *Config
	err      error
	msg      *Message
	br       *bufio.Reader
	read     int
	lastRead int
}

func newFileLogScanner(config *Config, r io.Reader) *fileLogScanner {
	return &fileLogScanner{
		config: config,
		br:     bufio.NewReaderSize(r, config.MaxChunkSize),
	}
}

func (s *fileLogScanner) Scan() bool {
	n, msg, err := msgFromReader(s.br)
	// fmt.Printf("scanned: %+v\n", msg)
	s.lastRead = n
	s.read += n
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

func (s *fileLogScanner) Message() *Message {
	return s.msg
}

func (s *fileLogScanner) Error() error {
	return s.err
}
