package client

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

type writerState uint32

const (
	_ writerState = iota

	stateClosed
	stateConnected
	stateFlushing
	stateFailing
)

func (s writerState) String() string {
	switch s {
	case stateClosed:
		return "CLOSED"
	case stateConnected:
		return "CONNECTED"
	case stateFlushing:
		return "FLUSHING"
	case stateFailing:
		return "FAILING"
	case 0:
		return "UNINITIALIZED"
	default:
		return "INVALID"
	}
}

type writerCmdType uint32

func (c writerCmdType) String() string {
	switch c {
	case cmdChanClosed:
		return "<command channel closed>"
	case cmdMsg:
		return "MESSAGE"
	case cmdFlush:
		return "FLUSH"
	case cmdClose:
		return "CLOSE"
	default:
		return "<invalid writerCmd value>"
	}
}

const (
	cmdChanClosed writerCmdType = iota
	cmdMsg
	cmdFlush
	cmdClose
)

type writerCmd struct {
	kind writerCmdType
	data []byte
	cb   WriterCallback
	res  *writerRes
}

type writerRes struct {
	off   uint64
	delta uint64
	err   error
}

func (wr *writerRes) vals(off, delta uint64, err error) *writerRes {
	wr.off, wr.delta, wr.err = off, delta, err
	return wr
}

var cachedFlushCmd = &writerCmd{kind: cmdFlush, res: &writerRes{}}
var cachedCloseCmd = &writerCmd{kind: cmdClose, res: &writerRes{}}

var cmdPool = sync.Pool{
	New: func() interface{} {
		return &writerCmd{res: &writerRes{}}
	},
}

// WriterCallback can be implemented in order to handle individual messages
// after the batch is flushed. It receives the message, batch offset, delta,
// and error, if any
type WriterCallback func(ctx context.Context, off, delta uint64, p []byte, err error)

type wrappedWriterCb func(cb WriterCallback)

// writeResult is used to send a callback back to the calling goroutine so it
// can be executed there.
// type writeResult chan WriterCallback

// MessageHandler represents a goroutine that handles messages after flush.
// Depending on whether x is set, it will immediately process all messages that
// come in, or messages can be manually waited on. If the handler is manually
// waiting, it can be in the same goroutine as the message writer.
type MessageHandler struct {
	conf        *config.Config
	writeResult chan wrappedWriterCb
	stopC       chan struct{}
	done        chan error
}

func (h *MessageHandler) ReadForever() {
	for {
		select {
		case cb := <-h.Next():
			fmt.Println(cb)
			// cb()
		}
	}
}

func (h *MessageHandler) Stop() error {
	h.stopC <- struct{}{}
	return <-h.done
}

func (h *MessageHandler) Next() chan wrappedWriterCb {
	return h.writeResult
}

// Writer writes message batches to the log server
type Writer struct {
	*Client
	conf         *Config
	gconf        *config.Config
	topic        []byte
	state        writerState
	resC         chan *writerRes
	flushcmd     *writerCmd
	stateManager StatePusher

	retries      int
	timer        *time.Timer
	timerStarted bool
	batch        *protocol.Batch // owned by client goroutine
	err          error
	inC          chan *writerCmd
	stopC        chan struct{}
}

// NewWriter returns a new instance of Writer for a topic
func NewWriter(conf *Config, topic string) *Writer {
	gconf := conf.ToGeneralConfig()
	w := &Writer{
		Client:   New(conf),
		conf:     conf,
		gconf:    gconf,
		topic:    []byte(topic),
		state:    stateClosed,
		timer:    time.NewTimer(-1),
		batch:    protocol.NewBatch(gconf),
		inC:      make(chan *writerCmd),
		resC:     make(chan *writerRes),
		flushcmd: &writerCmd{}, // cached for timer flush
		stopC:    make(chan struct{}),
	}

	go w.loop()
	return w
}

// WithStateHandler sets a state pusher on the writer. It should be called as
// part of initialization.
func (w *Writer) WithStateHandler(m StatePusher) *Writer {
	w.stateManager = m
	return w
}

// Reset sets the writer to its initial values
func (w *Writer) Reset(topic string) {
	w.topic = []byte(topic)
	w.batch.Reset()
	w.err = nil
	w.retries = 0
	w.stopTimer()
	w.timerStarted = false
	w.state = stateClosed
}

func (w *Writer) Write(p []byte) (int, error) {
	cmd := cmdPool.Get().(*writerCmd)
	cmd.kind = cmdMsg
	cmd.data = p

	res := w.doCommand(cmd)
	cmdPool.Put(cmd)
	if res.err != nil {
		return 0, res.err
	}
	return len(p), nil
}

// WriteAsync writes a message and calls cb when the batch is flushed.
// TODO this needs to accept a MessageHandler
func (w *Writer) WriteAsync(p []byte, handler *MessageHandler) (int, error) {
	return 0, nil
}

// Flush implements the LogWriter interface
func (w *Writer) Flush() error {
	res := w.doCommand(cachedFlushCmd)
	return res.err
}

// Close implements the LogWriter interface
func (w *Writer) Close() error {
	internal.Debugf(w.gconf, "closing writer")
	res := w.doCommand(cachedCloseCmd)
	w.stop()
	return res.err
}

func (w *Writer) doCommand(cmd *writerCmd) *writerRes {
	w.inC <- cmd

	res := <-w.resC
	return res
}

func (w *Writer) stopTimer() {
	if !w.timer.Stop() {
		select {
		case <-w.timer.C:
		default:
		}
	}
	w.timerStarted = false
}

func (w *Writer) loop() {
	for {
		select {
		case <-w.stopC:
			internal.Debugf(w.gconf, "<-stopC")
			return

		case cmd := <-w.inC:
			internal.Debugf(w.gconf, "inC <- %s", cmd.kind)
			if cmd == nil { // channel closed
				return
			}
			if w.err != nil {
				w.resC <- cmd.res.vals(0, 0, w.err)
				continue
			}

			var err error
			switch cmd.kind {
			case cmdMsg:
				err = w.handleMsg(cmd)
			case cmdFlush:
				err = w.handleFlush(cmd)
			case cmdClose:
				err = w.handleClose()
			default:
				log.Panicf("invalid command type: %v", cmd.kind)
			}

			w.err = err
			w.resC <- cmd.res.vals(0, 0, err)

		case <-w.timer.C:
			internal.Debugf(w.gconf, "<-timer.C %s", w.state)
			switch w.state {
			// case stateClosed:
			// 	w.stopTimer()
			case stateConnected:
				err := w.handleFlush(w.flushcmd)
				w.err = err
				if err == nil {
					w.resetTimer(w.conf.WaitInterval)
				}
			case stateFailing:
				w.err = w.handleReconnect()

				// case stateFlushing: // this should never happen
			}
		}
	}
}

func (w *Writer) handleMsg(cmd *writerCmd) error {
	// TODO this should probably check this is the first time we've tried and
	// if not, just continue retrying the connection.
	p := cmd.data
	if err := w.setErr(w.ensureConn()); err != nil {
		w.startReconnect()
		return err
	}

	if w.shouldFlush(len(p)) {
		if err := w.handleFlush(cmd); err != nil {
			return err
		}
	}

	if err := w.batch.Append(p); err != nil {
		return err
	}

	if !w.timerStarted {
		w.resetTimer(w.conf.WaitInterval)
		w.timerStarted = true
	}

	if cmd.cb != nil {
		// TODO add cb to an array along with the message bytes. they should
		// all be called after flush.
		// NOTE probably need a channel on the calling goroutine that it can
		// send the data back to when the batch has been flushed. that way the
		// code in the callback doesn't have to be executed in the connection's
		// goroutine.
	}

	return nil
}

func (w *Writer) shouldFlush(size int) bool {
	return (w.batch.CalcSize()+protocol.MessageSize(size) >= w.conf.BatchSize)
}

func (w *Writer) handleFlush(cmd *writerCmd) error {
	if w.err != nil {
		return w.err
	}

	batch := w.batch
	batch.SetTopic(w.topic)
	if batch.Empty() {
		return nil
	}

	w.state = stateFlushing
	off, err := w.Batch(batch)
	// TODO defer call all registered WriterCallbacks
	internal.Debugf(w.gconf, "flush complete, err: %+v", err)
	if serr := w.setErr(err); serr != nil {
		if w.stateManager != nil {
			perr := w.stateManager.Push(off, serr, batch.Copy())
			batch.Reset()
			if perr != nil {
				return perr
			}
		}
		batch.Reset()
		return err
	}
	batch.Reset()
	w.state = stateConnected

	if w.stateManager != nil {
		if perr := w.stateManager.Push(off, nil, nil); perr != nil {
			return perr
		}
	}
	return err
}

func (w *Writer) handleClose() error {
	if w.err != nil && w.Client.Conn != nil {
		return w.Client.Conn.Close()
	}
	internal.LogError(w.Client.flush())
	err := w.Client.Close()
	w.state = stateClosed
	return err
}

func (w *Writer) startReconnect() {
	w.stopTimer()
	w.resetTimer(w.conf.ConnRetryInterval)
	w.timerStarted = true
}

func (w *Writer) resetTimer(interval time.Duration) {
	if interval > 0 {
		w.timer.Reset(w.conf.ConnRetryInterval)
	}
}

func (w *Writer) handleReconnect() error {
	internal.Debugf(w.gconf, "attempting reconnect, attempt: %d", w.retries+1)
	if err := w.connect(w.conf.Hostport); err != nil {
		w.retries++
		if w.conf.ConnRetries > 0 && w.retries >= w.conf.ConnRetries {
			internal.Debugf(w.gconf, "giving up after %d attempts", w.retries+1)
			return err
		}

		interval := w.conf.ConnRetryInterval
		interval *= time.Duration(math.Round(float64(w.retries) * w.conf.ConnRetryMultiplier))
		if interval > w.conf.ConnRetryMaxInterval {
			interval = w.conf.ConnRetryMaxInterval
		}
		w.stopTimer()
		internal.Debugf(w.gconf, "trying again in %s", interval)
		w.resetTimer(interval)

		return err
	}

	internal.Debugf(w.gconf, "successfully reconnected after %d attempts", w.retries+1)
	w.retries = 0
	w.state = stateConnected
	w.stopTimer()
	w.resetTimer(w.conf.WaitInterval)
	return nil
}

func (w *Writer) setErr(err error) error {
	if err != nil {
		w.err = err
		w.state = stateFailing
	}
	return err
}

func (w *Writer) stop() {
	w.stopC <- struct{}{}
}
