package client

import (
	"bufio"
	"bytes"
	"io"

	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// ScannerV2 is used to read batches from the log, scanning message by message
type ScannerV2 struct {
	*ClientV2
	conf         *Config
	state        StatePuller
	s            *protocol.BatchScanner
	batch        *protocol.Batch
	msg          *protocol.MessageV2
	batchBuf     *bytes.Buffer
	batchBufBr   *bufio.Reader
	batchRead    int
	messagesRead int
	curr         uint64
	err          error
}

// NewScannerV2 returns a new instance of *ScannerV2
func NewScannerV2(conf *Config) *ScannerV2 {
	return &ScannerV2{
		conf:     conf,
		batchBuf: &bytes.Buffer{},
		msg:      protocol.NewMessageV2(conf.toGeneralConfig()),
	}
}

// ScannerForClientV2 returns a new scanner from a ClientV2
func ScannerForClientV2(c *ClientV2) *ScannerV2 {
	s := NewScannerV2(c.conf)
	s.ClientV2 = c
	return s
}

// DialScannerConfigV2 returns a new writer with a connection to addr
func DialScannerConfigV2(addr string, conf *Config) (*ScannerV2, error) {
	if addr == "" {
		addr = conf.Hostport
	}
	c, err := DialConfigV2(addr, conf)
	if err != nil {
		return nil, err
	}
	return ScannerForClientV2(c), nil
}

// DialScannerV2 returns a new scanner with a default configuration
func DialScannerV2(addr string) (*ScannerV2, error) {
	return DialScannerConfigV2(addr, DefaultConfig)
}

// Reset sets the scanner to it's initial values so it can be reused.
func (s *ScannerV2) Reset() {
	s.msg.Reset()
	s.curr = s.conf.Offset
	s.messagesRead = 0
	s.batchBuf.Reset()
	s.batchBufBr = bufio.NewReader(s.batchBuf)
	s.batchRead = 0
}

// Scan reads the next message. If it encounters an error, it returns false.
func (s *ScannerV2) Scan() bool {
	if s.s == nil { // initial state
		bs, err := s.ClientV2.ReadOffset(s.curr, s.conf.Limit)
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

			bs, err := s.ClientV2.ReadOffset(s.curr, s.conf.Limit)
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

func (s *ScannerV2) scanErr(err error) bool {
	if err != nil {
		internal.Logf("scan: %+v", err)
	}
	s.err = err
	return false
}

// Message returns the current message
func (s *ScannerV2) Message() *protocol.MessageV2 {
	return s.msg
}

func (s *ScannerV2) Error() error {
	return s.err
}
