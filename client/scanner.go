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
	statem            StatePuller
	curr              uint64
	err               error
	done              chan struct{}
}

// NewScanner returns a new instance of *Scanner
func NewScanner(conf *Config) *Scanner {
	return &Scanner{
		conf:     conf,
		batchBuf: &bytes.Buffer{},
		msg:      protocol.NewMessage(conf.ToGeneralConfig()),
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
	s.curr = 0
	s.messagesRead = 0
	s.totalMessagesRead = 0
	s.batch.Reset()
	s.batchBuf.Reset()
	s.batchBufBr = bufio.NewReader(s.batchBuf)
	s.msg.Reset()
	s.batchRead = 0
	s.batchesRead = 0
	s.nbatches = 0
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
	if err := s.readMessage(); err != nil {
		return s.scanErr(err)
	}
	return true
}

// Complete marks the current message processing completed. It will panic if no
// StatePuller exists on the scanner. It will pass the error back from the
// StatePuller.
func (s *Scanner) Complete() error {
	off := s.curr
	delta := s.batchRead
	if s.batch != nil && uint64(s.batchRead) >= s.batch.Size {
		fullsize, _ := s.batch.FullSize()
		off = s.curr + uint64(fullsize)
		delta = 0
	}
	internal.Debugf(s.gconf, "completing to offset %d, delta %d", off, delta)
	return s.statem.Complete(off, uint64(delta))
}

func (s *Scanner) readMessage() error {
	s.msg.Reset()
	n, err := s.msg.ReadFrom(s.batchBufBr)
	if err != nil {
		return err
	}
	s.batchRead += int(n)
	s.messagesRead++
	s.totalMessagesRead++
	return err
}

func (s *Scanner) doInitialRead() error {
	var bs *protocol.BatchScanner
	var err error
	var nbatches int
	var hasState bool
	var delta uint64

	if s.statem != nil {
		_, _, err := s.statem.Get()
		if err == nil {
			hasState = true
		}
	}

	if s.conf.UseTail { // offset was not explicitly set
		if hasState {
			off, delt, err := s.statem.Get()
			delta = delt
			if err != nil {
				return err
			}
			s.curr = off
			internal.Debugf(s.gconf, "starting from previous state: offset %d, delta %d", off, delta)
			nbatches, bs, err = s.Client.ReadOffset(s.topic, s.curr, s.conf.Limit)
			if err != nil {
				return err
			}
		} else {
			s.curr, nbatches, bs, err = s.Client.Tail(s.topic, s.conf.Limit)
		}
	} else {
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

	for uint64(s.batchRead) < delta {
		if err := s.readMessage(); err != nil {
			return err
		}
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

func (s *Scanner) Close() error {
	internal.LogError(s.Client.Close())
	if m, ok := s.statem.(internal.LifecycleManager); ok {
		internal.LogError(m.Shutdown())
	}
	return nil
}
