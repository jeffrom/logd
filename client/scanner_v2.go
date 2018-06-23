package client

import "github.com/jeffrom/logd/protocol"

// ScannerV2 is used to read batches from the log, scanning message by message
type ScannerV2 struct {
	*ClientV2
	conf  *Config
	state StatePuller
	s     *protocol.BatchScanner
	msg   *protocol.MessageV2
	err   error
}

// NewScannerV2 returns a new instance of *ScannerV2
func NewScannerV2(conf *Config) *ScannerV2 {
	return &ScannerV2{
		conf: conf,
	}
}

// Reset sets the scanner to it's initial values so it can be reused.
func (s *ScannerV2) Reset() {
	s.msg.Reset()
}

// Scan reads the next message. If it encounters an error, it returns false.
func (s *ScannerV2) Scan() bool {
	return false
}

// Message returns the current message
func (s *ScannerV2) Message() *protocol.MessageV2 {
	return s.msg
}

func (s *ScannerV2) Error() error {
	return s.err
}
