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
	s.msg = nil
	n, msg, err := msgFromReader(s.br)
	// fmt.Printf("scanned (%d): %+v %v\n", n, msg, err)
	s.lastRead = n
	s.read += n
	s.err = err

	s.msg = msg
	return err == nil
}

func (s *fileLogScanner) Message() *Message {
	return s.msg
}

func (s *fileLogScanner) Error() error {
	return s.err
}
