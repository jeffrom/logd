package client

import (
	"errors"
	"log"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
)

// Logger writes logs to a temporary buffer, batching write commands to the
// server.
type Logger struct {
	*Client
	cmd       *protocol.Command
	batch     [][]byte
	batchsize int
	flushTime int32
	timer     *time.Timer
}

// DialLogger returns a new client logger
func DialLogger(addr string) (*Logger, error) {
	return DialLoggerConfig(addr, config.DefaultConfig)
}

// DialLoggerConfig returns a new client logger with options
func DialLoggerConfig(addr string, conf *config.Config) (*Logger, error) {
	c, err := DialConfig(addr, conf)
	if err != nil {
		return nil, err
	}
	l := &Logger{
		Client: c,
		cmd:    protocol.NewCommand(conf, protocol.CmdMessage),
		batch:  make([][]byte, conf.ClientChunkSize),
		timer:  time.NewTimer(time.Duration(conf.ClientChunkInterval)),
	}
	return l, nil
}

func (l *Logger) Write(b []byte) (int, error) {
	if l.shouldFlush() {
		err := l.Flush()
		if err != nil {
			l.batchsize = 0
			return len(b), nil
		}
		return 0, err
	}

	l.addBatch(b)
	return len(b), nil
}

// Flush writes any pending batches
func (l *Logger) Flush() error {
	defer l.resetTimer()
	l.cmd.Args = l.batch[:l.batchsize]
	resp, err := l.Client.Do(l.cmd)
	if err != nil {
		return err
	}
	if resp.Failed() {
		log.Printf("client logger flush failed: %s", resp.Body)
		return errors.New("write command failed")
	}
	return nil
}

func (l *Logger) shouldFlush() bool {
	if l.batchsize >= l.config.ClientChunkSize {
		return true
	}
	select {
	case <-l.timer.C:
		return true
	default:
	}
	return false
}

func (l *Logger) addBatch(b []byte) {
	l.batch[l.batchsize] = b
	l.batchsize++
}

func (l *Logger) resetTimer() {
	if !l.timer.Stop() {
		<-l.timer.C
	}
	l.timer.Reset(time.Duration(l.config.ClientChunkInterval))
}

// messageBatch collects messages so they can be sent in a batch
type messageBatch struct {
	config *config.Config
}

func newMessageBatch(conf *config.Config) *messageBatch {
	return &messageBatch{config: conf}
}
