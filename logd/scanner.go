package logd

import (
	"bufio"
	"bytes"
	stderrors "errors"
	"io"
	"time"

	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/pkg/errors"
)

// ErrStopped indicates the scanner was stopped
var ErrStopped = stderrors.New("stopped")

// Scanner is used to read batches from the log, scanning message by message
type Scanner struct {
	Client            *Client
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
	currLocal         uint64
	currRemote        uint64
	err               error
	done              chan struct{}
	pollC             chan error
	usetail           bool
	startoff          uint64
	limit             int
}

// NewScanner returns a new instance of *Scanner.
func NewScanner(conf *Config, topic string) *Scanner {
	s := &Scanner{
		conf:     conf,
		batchBuf: &bytes.Buffer{},
		msg:      protocol.NewMessage(conf.BatchSize),
		done:     make(chan struct{}),
		pollC:    make(chan error),
		usetail:  conf.UseTail,
		startoff: conf.Offset,
		limit:    conf.Limit,
		statem:   NoopStatePuller(0),
	}

	if topic != "" {
		s.Client = New(conf)
		s.topic = []byte(topic)
	}

	return s
}

// ScannerForClient returns a new scanner from a Client
func ScannerForClient(c *Client) *Scanner {
	s := NewScanner(c.conf, "")
	s.Client = c
	return s
}

// DialScannerConfig returns a new writer with a connection to addr
func DialScannerConfig(addr string, conf *Config) (*Scanner, error) {
	if addr == "" {
		addr = conf.Host
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
	s.currLocal = 0
	s.currRemote = 0
	s.messagesRead = 0
	s.totalMessagesRead = 0
	if s.batch != nil {
		s.batch.Reset()
	}
	s.batchBuf.Reset()
	s.batchBufBr = bufio.NewReader(s.batchBuf)
	s.msg.Reset()
	s.batchRead = 0
	s.batchesRead = 0
	s.nbatches = 0
	s.s = nil
	s.usetail = s.conf.UseTail
	s.startoff = s.conf.Offset
	s.limit = s.conf.Limit

	select {
	case <-s.done:
	default:
	}
}

func (s *Scanner) resetDoneC() {
	select {
	case <-s.done: // if it's closed, this will recreate it
		s.done = make(chan struct{})
	default:
	}
}

// WithSetStatePuller sets the StatePuller on the Scanner.
func (s *Scanner) WithStateHandler(statem StatePuller) *Scanner {
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

func (s *Scanner) UseTail() {
	if s.messagesRead > 0 {
		panic("attempted to set offset while already scanning")
	}
	s.usetail = true
}

func (s *Scanner) SetOffset(off uint64) {
	if s.messagesRead > 0 {
		panic("attempted to set offset while already scanning")
	}
	s.usetail = false
	s.startoff = off
}

func (s *Scanner) SetLimit(n int) {
	s.limit = n
}

// Scan reads the next message. If it encounters an error, it returns false.
func (s *Scanner) Scan() bool {
	if !s.conf.ReadForever && s.totalMessagesRead >= s.limit {
		return s.scanErr(nil)
	}

	if s.s == nil { // initial state
		s.resetDoneC()
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

// Start marks the current message as processing. It will return ErrProcessing
// if the message is already being processed.
func (s *Scanner) Start() error {
	return s.statem.Start(s.currRemote, uint64(s.batchRead))
}

// Complete marks the current message processing completed. It will panic if no
// StatePuller exists on the scanner.
func (s *Scanner) Complete(err error) error {
	off := s.currRemote
	delta := s.batchRead
	if s.batch != nil && s.batchRead >= s.batch.Size {
		fullsize, _ := s.batch.FullSize()
		off = s.currRemote + uint64(fullsize)
		delta = 0
	}
	if err == nil {
		err = s.Error()
	}
	internal.Debugf(s.Client.gconf, "completing to offset %d, delta %d (err: %+v)", off, delta, err)
	return s.statem.Complete(off, uint64(delta), err)
}

func (s *Scanner) readMessage() error {
	s.msg.Reset()
	n, err := s.msg.ReadFrom(s.batchBufBr)
	if err != nil {
		return err
	}
	s.msg.Offset = s.currLocal
	s.msg.Delta = uint64(s.batchRead)

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

	if s.usetail { // offset was not explicitly set
		if hasState {
			off, delt, err := s.statem.Get()
			delta = delt
			if err != nil {
				return err
			}
			s.currRemote = off
			internal.Debugf(s.Client.gconf, "starting from previous state: offset %d, delta %d", off, delta)
			nbatches, bs, err = s.Client.ReadOffset(s.topic, s.currRemote, s.limit)
			if err != nil {
				return err
			}
		} else {
			s.currRemote, nbatches, bs, err = s.Client.Tail(s.topic, s.limit)
			internal.Debugf(s.Client.gconf, "starting with %d batches from log tail at %d (err: %+v)", nbatches, s.currRemote, err)
		}
	} else {
		s.currRemote = s.startoff
		nbatches, bs, err = s.Client.ReadOffset(s.topic, s.currRemote, s.limit)
		internal.Debugf(s.Client.gconf, "starting with %d batches from offset %d (err: %+v)", nbatches, s.currRemote, err)
	}
	if err != nil {
		return err
	}
	s.currLocal = s.currRemote

	s.s = bs
	s.nbatches = nbatches
	internal.Debugf(s.Client.gconf, "started reading from %d", s.currRemote)
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
	if s.batchRead >= s.batch.Size {
		if s.batchesRead >= s.nbatches {
			err := s.requestMoreBatches(false)
			if err == protocol.ErrNotFound {
				if !s.conf.ReadForever {
					return protocol.ErrNotFound
				}
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
	close(s.done)
	s.s = nil
}

func (s *Scanner) requestMoreBatches(poll bool) error {
	select {
	case <-s.done:
		return ErrStopped
	default:
	}

	if !poll {
		s.currRemote += uint64(s.Client.bs.Scanned())
	}
	nbatches, bs, err := s.Client.ReadOffset(s.topic, s.currRemote, s.limit)
	internal.Debugf(s.Client.gconf,
		"requested more batches from %d. read %d messages (%d/%d bytes) (err: %+v)",
		s.currRemote, s.messagesRead, s.batchRead, s.batch.Size, err)
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
	if size, ok := s.batch.FullSize(); ok {
		s.currLocal += uint64(size)
	}

	s.batch = s.s.Batch()
	s.batchRead = 0
	s.batchesRead++
	s.batchBuf.Reset()
	s.batchBufBr = bufio.NewReader(s.batchBuf)
	_, err := s.batchBuf.Write(s.batch.MessageBytes())
	return err
}

func (s *Scanner) pollBatch() error {
	go func() {
		for {
			select {
			case <-time.After(s.conf.WaitInterval):
				err := s.requestMoreBatches(true)
				if err != nil {
					if err == protocol.ErrNotFound {
						continue
					}
					s.pollC <- err
					return
				}
				s.pollC <- nil
				return
			case <-s.done:
				s.pollC <- ErrStopped
				return
			}
		}
	}()

	return <-s.pollC
}

func (s *Scanner) scanErr(err error) bool {
	if err != nil {
		if errors.Cause(err) == protocol.ErrNotFound {
			internal.Debugf(s.Client.gconf, "scan: %+v", err)
		} else {
			internal.Logf("scan: %+v", err)
		}
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
	internal.IgnoreError(s.conf.Verbose, s.Client.Close())
	if m, ok := s.statem.(internal.LifecycleManager); ok {
		internal.LogError(m.Shutdown())
	}
	return nil
}
