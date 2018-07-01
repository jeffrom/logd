package client

import (
	"bufio"
	"bytes"
	"io"

	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Scanner is used to read batches from the log, scanning message by message
type Scanner struct {
	*Client
	conf         *Config
	state        StatePuller
	s            *protocol.BatchScanner
	batch        *protocol.Batch
	msg          *protocol.Message
	batchBuf     *bytes.Buffer
	batchBufBr   *bufio.Reader
	batchRead    int
	messagesRead int
	curr         uint64
	err          error
}

// NewScanner returns a new instance of *Scanner
func NewScanner(conf *Config) *Scanner {
	return &Scanner{
		conf:     conf,
		batchBuf: &bytes.Buffer{},
		msg:      protocol.NewMessage(conf.toGeneralConfig()),
	}
}

// ScannerForClient returns a new scanner from a Client
func ScannerForClient(c *Client) *Scanner {
	s := NewScanner(c.conf)
	s.Client = c
	return s
}

// DialScannerConfig returns a new writer with a connection to addr
func DialScannerConfig(addr string, conf *Config) (*Scanner, error) {
	if addr == "" {
		addr = conf.Hostport
	}
	c, err := DialConfig(addr, conf)
	if err != nil {
		return nil, err
	}
	return ScannerForClient(c), nil
}

// DialScanner returns a new scanner with a default configuration
func DialScanner(addr string) (*Scanner, error) {
	return DialScannerConfig(addr, DefaultConfig)
}

// Reset sets the scanner to it's initial values so it can be reused.
func (s *Scanner) Reset() {
	s.msg.Reset()
	s.curr = s.conf.Offset
	s.messagesRead = 0
	s.batchBuf.Reset()
	s.batchBufBr = bufio.NewReader(s.batchBuf)
	s.batchRead = 0
}

// Scan reads the next message. If it encounters an error, it returns false.
func (s *Scanner) Scan() bool {
	if s.s == nil { // initial state
		bs, err := s.Client.ReadOffset(s.curr, s.conf.Limit)
		if err != nil {
			return s.scanErr(err)
		}
		s.s = bs
		s.s.Batch().MessageBytes()
	}

	// if we've finished reading a batch, maybe scan another batch or make
	// another request for more batches
	if s.batchRead >= len(s.s.Batch().MessageBytes()) {
		s.curr += uint64(s.batchRead)
		if !s.conf.ReadForever && s.messagesRead >= s.conf.Limit {
			return s.scanErr(nil)
		}

		ok := s.bs.Scan()
		if !ok {
			err := s.bs.Error()
			if err != nil && err != io.EOF {
				return s.scanErr(err)
			}

			bs, err := s.Client.ReadOffset(s.curr, s.conf.Limit)
			if err != nil {
				return s.scanErr(err)
			}
			s.s = bs
		}

		s.batch = s.bs.Batch()
		s.batchRead = 0
		s.batchBuf.Reset()
		s.batchBufBr = bufio.NewReader(s.batchBuf)
		if _, err := s.batchBuf.Write(s.batch.MessageBytes()); err != nil {
			return s.scanErr(err)
		}
	}

	// read the next message in the batch
	s.msg.Reset()
	n, err := s.msg.ReadFrom(s.batchBufBr)
	s.batchRead += int(n)
	if err != nil {
		return s.scanErr(err)
	}
	s.messagesRead++
	return true
}

func (s *Scanner) scanErr(err error) bool {
	if err != nil {
		internal.Logf("scan: %+v", err)
	}
	s.err = err
	return false
}

// Message returns the current message
func (s *Scanner) Message() *protocol.Message {
	return s.msg
}

func (s *Scanner) Error() error {
	return s.err
}
