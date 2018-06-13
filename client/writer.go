package client

import (
	"sync"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Writer is used for sending messages to the log over a tcp socket
type Writer struct {
	*ClientV2
	conf  *Config
	gconf *config.Config
	state StatePusher

	stopC      chan struct{}
	flushSyncC chan struct{}
	readySyncC chan error
	mu         sync.Mutex
	batch      *protocol.Batch
}

// NewWriter returns a new instance of Writer
func NewWriter(conf *Config) *Writer {
	gconf := conf.toGeneralConfig()
	w := &Writer{
		conf:       conf,
		gconf:      gconf,
		stopC:      make(chan struct{}),
		flushSyncC: make(chan struct{}),
		readySyncC: make(chan error),
		batch:      protocol.NewBatch(gconf),
	}
	w.start()
	return w
}

// ForClientV2 returns a new writer from a *ClientV2
func ForClientV2(c *ClientV2) *Writer {
	w := NewWriter(c.conf)
	w.ClientV2 = c
	return w
}

// DialWriterConfig returns a new writer with a connection to addr
func DialWriterConfig(addr string, conf *Config) (*Writer, error) {
	if addr == "" {
		addr = conf.Hostport
	}
	c, err := DialConfigV2(addr, conf)
	if err != nil {
		return nil, err
	}

	return ForClientV2(c), nil
}

// DialWriter returns a new writer with a default configuration
func DialWriter(addr string) (*Writer, error) {
	return DialWriterConfig(addr, DefaultConfig)
}

// SetStateHandler sets a state handler on the writer
func (w *Writer) SetStateHandler(h StatePusher) {
	w.state = h
}

// Reset sets the Writer to its initial values
func (w *Writer) Reset() {
	w.stop()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batch.Reset()
	w.start()
}

func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	shouldFlush := w.shouldFlush(len(p))
	if shouldFlush {
		w.mu.Unlock()
		err := w.signalFlushSync()
		w.mu.Lock()

		if err != nil {
			return 0, err
		}
	}
	defer w.mu.Unlock()

	if err := w.batch.Append(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// func (w *Writer) swap() {
// 	w.batch, w.batchb = w.batchb, w.batch
// 	w.batch.Reset()
// }

func (w *Writer) signalFlushSync() error {
	internal.Debugf(w.gconf, "signalFlushSync")
	select {
	case w.flushSyncC <- struct{}{}:
	}

	select {
	case err := <-w.readySyncC:
		return err
	}
}

// Flush implements the LogWriterV2 interface
func (w *Writer) Flush() error {
	return w.signalFlushSync()
}

// Close implements the LogWriterV2 interface
func (w *Writer) Close() error {
	// w.mu.Lock()
	// defer w.mu.Unlock()
	internal.IgnoreError(w.ClientV2.Close())
	return nil
}

func (w *Writer) start() {
	go func() {
		for {
			internal.Debugf(w.gconf, "Writer flusher waiting for event")
			select {
			case <-w.stopC:
				internal.Debugf(w.gconf, "<-stopC")
				internal.IgnoreError(w.flushPending(false))
				return
			// case <-w.flushC:
			// 	internal.Debugf(w.gconf, "<-flushC")
			// 	internal.IgnoreError(w.flushPending(false))
			case <-w.flushSyncC:
				internal.Debugf(w.gconf, "<-flushSyncC")
				internal.IgnoreError(w.flushPending(true))
			case <-time.After(w.conf.WaitInterval):
				internal.Debugf(w.gconf, "<-WaitInterval")
				internal.IgnoreError(w.flushPending(false))
			}

		}
	}()
}

func (w *Writer) stop() {
	w.stopC <- struct{}{}
}

func (w *Writer) signalReadySync(err error, sync bool) {
	if !sync {
		return
	}
	w.readySyncC <- err
	internal.Debugf(w.gconf, "<-readySyncC")
}

func (w *Writer) flushPending(sync bool) error {
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
	}()
	internal.Debugf(w.gconf, "flushing %v: sync: %t", w.batch, sync)
	batch := w.batch
	var err error

	if batch.Messages <= 0 {
		w.signalReadySync(err, sync)
		return nil
	}

	off, err := w.Batch(batch)
	internal.Debugf(w.gconf, "flush complete")
	batch.Reset()
	if err != nil {
		w.signalReadySync(err, sync)
		return err
	}

	if w.state != nil {
		internal.IgnoreError(w.state.Push(off))
	}
	w.signalReadySync(err, sync)
	return err
}

func (w *Writer) shouldFlush(size int) bool {
	// fmt.Printf("shouldFlush: %d + %d (%d) >= %d\n", w.batch.Size, size, w.batch.Size+uint64(size), w.conf.BatchSize)
	should := (w.batch.CalcSize()+protocol.MessageSize(size) >= w.conf.BatchSize)
	return should
}
