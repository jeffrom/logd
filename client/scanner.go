package client

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// ErrStopped indicates the scanner was stopped
var ErrStopped = errors.New("stopped")

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
	batchesRead  int
	nbatches     int
	messagesRead int
	curr         uint64
	err          error
	done         chan struct{}
}

// NewScanner returns a new instance of *Scanner
func NewScanner(conf *Config) *Scanner {
	return &Scanner{
		conf:     conf,
		batchBuf: &bytes.Buffer{},
		msg:      protocol.NewMessage(conf.toGeneralConfig()),
		done:     make(chan struct{}),
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
	s.batchesRead = 0
	s.nbatches = 0

	select {
	case <-s.done:
	default:
	}
}

// Scan reads the next message. If it encounters an error, it returns false.
func (s *Scanner) Scan() bool {
	if !s.conf.ReadForever && s.messagesRead >= s.conf.Limit {
		return s.scanErr(nil)
	}

	if s.s == nil { // initial state
		var bs *protocol.BatchScanner
		var err error
		var nbatches int
		if s.conf.UseTail {
			s.curr, nbatches, bs, err = s.Client.Tail(s.conf.Limit)
		} else {
			nbatches, bs, err = s.Client.ReadOffset(s.curr, s.conf.Limit)
		}
		if err != nil {
			return s.scanErr(err)
		}
		s.s = bs
		s.nbatches = nbatches
		internal.Debugf(s.gconf, "started reading from %d", s.curr)

		if !s.s.Scan() {
			return s.scanErr(s.s.Error())
		}

		if err := s.setNextBatch(); err != nil {
			return s.scanErr(err)
		}
	} else if s.messagesRead >= s.batch.Messages {
		if !s.conf.ReadForever && s.messagesRead >= s.conf.Limit {
			return s.scanErr(nil)
		}

		if s.batchesRead >= s.nbatches {
			if err := s.requestMoreBatches(false); err == protocol.ErrNotFound {
				if perr := s.pollBatch(); perr != nil {
					return s.scanErr(perr)
				}
			} else if err != nil {
				return s.scanErr(err)
			}
		}

		ok := s.s.Scan()
		if !ok {
			err := s.s.Error()
			if err != nil && err != io.EOF {
				return s.scanErr(err)
			}
		}

		if err := s.setNextBatch(); err != nil {
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

// Stop causes the scanner to stop making requests and returns ErrStopped
func (s *Scanner) Stop() {
	s.done <- struct{}{}
}

func (s *Scanner) requestMoreBatches(poll bool) error {
	select {
	case <-s.done:
		return ErrStopped
	default:
	}

	if !poll {
		s.curr += uint64(s.bs.Scanned())
	}
	nbatches, bs, err := s.Client.ReadOffset(s.curr, s.conf.Limit)
	if err != nil {
		return err
	}
	s.s = bs
	s.messagesRead = 0
	s.batchesRead = 0
	s.nbatches = nbatches

	return nil
}

func (s *Scanner) setNextBatch() error {
	s.batch = s.s.Batch()
	s.batchRead = 0
	s.batchesRead++
	s.batchBuf.Reset()
	s.batchBufBr = bufio.NewReader(s.batchBuf)
	if _, err := s.batchBuf.Write(s.batch.MessageBytes()); err != nil {
		return err
	}
	return nil
}

func (s *Scanner) pollBatch() error {
	for {
		time.Sleep(s.conf.WaitInterval)
		err := s.requestMoreBatches(true)
		if err != nil {
			if err == protocol.ErrNotFound {
				continue
			}
			return err
		}
		return nil
	}
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
