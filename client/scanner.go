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
	conf              *Config
	topic             []byte
	s                 *protocol.BatchScanner
	batch             *protocol.Batch
	msg               *protocol.Message
	batchBuf          *bytes.Buffer
	batchBufBr        *bufio.Reader
	batchRead         int
	batchesRead       int
	nbatches          int
	messagesRead      int
	totalMessagesRead int
	state             *scannerState
	statem            StatePuller
	curr              uint64
	err               error
	done              chan struct{}
}

type scannerState struct {
	offset uint64
	delta  uint64
}

// NewScanner returns a new instance of *Scanner
func NewScanner(conf *Config) *Scanner {
	return &Scanner{
		conf:     conf,
		batchBuf: &bytes.Buffer{},
		msg:      protocol.NewMessage(conf.ToGeneralConfig()),
		state:    &scannerState{},
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
	s.totalMessagesRead = 0
	s.batchBuf.Reset()
	s.batchBufBr = bufio.NewReader(s.batchBuf)
	s.batchRead = 0
	s.batchesRead = 0
	s.nbatches = 0
	s.state.offset = 0
	s.state.delta = 0
	s.s = nil

	select {
	case <-s.done:
	default:
	}
}

// WithSetStatePuller sets the StatePuller on the Scanner
func (s *Scanner) WithStatePuller(statem StatePuller) *Scanner {
	s.statem = statem
	return s
}

// SetTopic sets the topic for the scanner.
func (s *Scanner) SetTopic(topic string) {
	s.topic = []byte(topic)
}

// Topic returns the topic for the scanner.
func (s *Scanner) Topic() string {
	return string(s.topic)
}

// Scan reads the next message. If it encounters an error, it returns false.
func (s *Scanner) Scan() bool {
	if !s.conf.ReadForever && s.totalMessagesRead >= s.conf.Limit {
		return s.scanErr(nil)
	}

	if s.s == nil { // initial state
		if err := s.doInitialRead(); err != nil {
			return s.scanErr(err)
		}
	} else if err := s.scanNextBatch(); err != nil {
		return s.scanErr(err)
	}

	// read the next message in the batch
	s.msg.Reset()
	n, err := s.msg.ReadFrom(s.batchBufBr)
	s.batchRead += int(n)
	if err != nil {
		return s.scanErr(err)
	}
	s.messagesRead++
	s.totalMessagesRead++
	return true
}

func (s *Scanner) doInitialRead() error {
	var bs *protocol.BatchScanner
	var err error
	var nbatches int
	if s.conf.UseTail {
		s.curr, nbatches, bs, err = s.Client.Tail(s.topic, s.conf.Limit)
	} else {
		// TODO test that s.conf.Offset is respected
		s.curr = s.conf.Offset
		nbatches, bs, err = s.Client.ReadOffset(s.topic, s.curr, s.conf.Limit)
	}
	if err != nil {
		return err
	}
	s.s = bs
	s.nbatches = nbatches
	internal.Debugf(s.gconf, "started reading from %d", s.curr)
	if !s.s.Scan() {
		return s.s.Error()
	}

	if err := s.setNextBatch(); err != nil {
		return err
	}
	return err
}

func (s *Scanner) scanNextBatch() error {
	if s.messagesRead >= s.batch.Messages || s.batchRead >= int(s.batch.Size) {
		if !s.conf.ReadForever && s.totalMessagesRead >= s.conf.Limit {
			return nil
		}

		if s.batchesRead >= s.nbatches {
			if err := s.requestMoreBatches(false); err == protocol.ErrNotFound {
				if perr := s.pollBatch(); perr != nil {
					return perr
				}
			} else if err != nil {
				return err
			}
		}

		if !s.s.Scan() {
			err := s.s.Error()
			if err != nil && err != io.EOF {
				return err
			}
		}

		if err := s.setNextBatch(); err != nil {
			return err
		}
	}

	return nil
}

// func (s *Scanner) finishedBatch() bool {
// 	if s.messagesRead >= s.batch.Messages {
// 		return true
// 	}
// 	if s.statem != nil {
// 		size, _ := s.batch.FullSize()
// 		_, read, err := s.statem.Get()

// 	}
// 	return false
// }

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
	nbatches, bs, err := s.Client.ReadOffset(s.topic, s.curr, s.conf.Limit)
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
	_, err := s.batchBuf.Write(s.batch.MessageBytes())
	return err
}

func (s *Scanner) pollBatch() error {
	if !s.conf.ReadForever {
		return protocol.ErrNotFound
	}
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
