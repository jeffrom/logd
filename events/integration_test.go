package events

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logd"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
	"github.com/pkg/errors"
)

func newIntegrationTestClientConfig(verbose bool) *logd.Config {
	c := logd.NewConfig()
	*c = *logd.DefaultConfig

	c.Verbose = verbose
	c.BatchSize = 1024 * 20

	c.ConnRetryInterval /= 4
	c.ConnRetries = 3
	return c
}

type integrationTest struct {
	conf  *config.Config
	cconf *logd.Config
	offs  []uint64
	mu    sync.Mutex

	n        int
	h        *Handlers
	writers  []*logd.Writer
	scanners []*logd.Scanner

	failed []*protocol.Batch
}

func newIntegrationTestState(conf *config.Config, cconf *logd.Config, n int) *integrationTest {
	return &integrationTest{
		conf:     conf,
		cconf:    cconf,
		n:        n,
		offs:     []uint64{},
		writers:  make([]*logd.Writer, 0),
		scanners: make([]*logd.Scanner, 0),
		failed:   make([]*protocol.Batch, 0),
	}
}

func (ts *integrationTest) addOff(off uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.offs = append(ts.offs, off)
}

func (ts *integrationTest) setup(t *testing.T) {
	if ts.conf.MaxBatchSize > ts.cconf.BatchSize {
		ts.cconf.BatchSize = ts.conf.MaxBatchSize
	} else {
		ts.conf.MaxBatchSize = ts.cconf.BatchSize
	}

	ts.conf.Host = ":0"
	ts.conf.HttpHost = ""
	ts.h = NewHandlers(ts.conf)
	doStartHandler(t, ts.h)

	ts.cconf.Hostport = ts.h.servers[0].ListenAddr().String()
	for i := 0; i < ts.n; i++ {
		w := logd.NewWriter(ts.cconf, "default")
		w.WithStateHandler(ts)

		s := logd.NewScanner(ts.cconf, "default")

		ts.writers = append(ts.writers, w)
		ts.scanners = append(ts.scanners, s)
	}
}

func (ts *integrationTest) shutdown(t *testing.T) {
	t.Helper()

	defer doShutdownHandler(t, ts.h)
	for _, s := range ts.scanners {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}
	for _, w := range ts.writers {
		if err := w.Close(); err != nil {
			if !logd.IsRetryable(err) {
				t.Error(err)
			} else {
				t.Logf("error closing writer ignored: %+v", err)
			}
		}
	}
}

// Push implements logd.StatePusher.
func (ts *integrationTest) Push(off uint64) error {
	// if err != nil {
	// 	ts.mu.Lock()
	// 	ts.failed = append(ts.failed, batch)
	// 	ts.mu.Unlock()
	// 	return err
	// }

	ts.addOff(off)

	return nil
}

// Close implements logd.StatePusher.
func (ts *integrationTest) Close() error {
	return nil
}

func TestIntegrationWriteRead(t *testing.T) {
	conf := testhelper.IntegrationTestConfig(testing.Verbose())
	cconf := newIntegrationTestClientConfig(testing.Verbose())

	ts := newIntegrationTestState(conf, cconf, 1)
	ts.setup(t)
	defer ts.shutdown(t)

	testIntegrationWriter(t, ts)
}

func TestIntegrationWriteRead4(t *testing.T) {
	conf := testhelper.IntegrationTestConfig(testing.Verbose())
	cconf := newIntegrationTestClientConfig(testing.Verbose())

	ts := newIntegrationTestState(conf, cconf, 4)
	ts.setup(t)
	defer ts.shutdown(t)

	testIntegrationWriter(t, ts)
}

func TestIntegrationReconnect(t *testing.T) {
	conf := testhelper.IntegrationTestConfig(testing.Verbose())
	cconf := newIntegrationTestClientConfig(testing.Verbose())
	cconf.ConnectTimeout = 100 * time.Millisecond

	ts := newIntegrationTestState(conf, cconf, 1)
	ts.setup(t)
	defer ts.shutdown(t)

	testIntegrationReconnect(t, ts)
}

func TestIntegrationReconnect4(t *testing.T) {
	conf := testhelper.IntegrationTestConfig(testing.Verbose())
	cconf := newIntegrationTestClientConfig(testing.Verbose())
	cconf.ConnectTimeout = 100 * time.Millisecond

	ts := newIntegrationTestState(conf, cconf, 4)
	ts.setup(t)
	defer ts.shutdown(t)

	testIntegrationReconnect(t, ts)
}

func testIntegrationWriter(t *testing.T, ts *integrationTest) {
	n := 10000
	errC := make(chan error, ts.n)
	wg := sync.WaitGroup{}

	var wrote int32
	for _, w := range ts.writers {
		// s := ts.scanners[n]
		wg.Add(1)

		go func(w *logd.Writer) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				_, err := w.Write([]byte("oh dang sup"))
				if err != nil {
					errC <- errors.Wrap(err, "write failed")
					return
				}
				atomic.AddInt32(&wrote, 1)
			}

			if err := w.Flush(); err != nil {
				errC <- errors.Wrap(err, "flush failed")
				return
			}

		}(w)
	}

	wg.Wait()
	failOnErrors(t, errC)
	if int(atomic.LoadInt32(&wrote)) != n*ts.n {
		t.Errorf("expected to write %d but wrote %d", n*ts.n, atomic.LoadInt32(&wrote))
	}
	// ts.h.h["default"].topic.logw.Flush()

	var read int32
	for _, s := range ts.scanners {
		wg.Add(1)

		go func(s *logd.Scanner) {
			defer wg.Done()

			s.Reset()
			s.UseTail()
			s.SetLimit(int(atomic.LoadInt32(&wrote)) + 10)
			passed := false
			for s.Scan() {
				passed = true
				atomic.AddInt32(&read, 1)
			}
			if err := s.Error(); err != nil && int(atomic.LoadInt32(&read)) < (int(atomic.LoadInt32(&wrote))/n) {
				t.Logf("expected to read %d but read %d (err: %+v)", atomic.LoadInt32(&wrote), atomic.LoadInt32(&read), err)
				errC <- errors.Wrap(err, "scan failed")
				return
			} else if err != nil {
				t.Logf("scan error: %+v", err)
			}

			if !passed {
				errC <- errors.New("didn't scan anything")
				return
			}
		}(s)
	}

	wg.Wait()
	// if atomic.LoadInt32(&read) != atomic.LoadInt32(&wrote)*int32(ts.n) {
	// 	t.Fatalf("wrote %d messages but read %d", wrote, read)
	// }
	failOnErrors(t, errC)
}

func testIntegrationReconnect(t *testing.T, ts *integrationTest) {
	n := 50
	runs := 1
	if !testing.Short() {
		runs = 100
	}
	for i := 0; i < runs; i++ {
		errC := make(chan error, ts.n)
		wg := sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 10; i++ {
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				if err := ts.h.Stop(); err != nil {
					errC <- errors.Wrap(err, "shutdown failed")
				}
				// time.Sleep(time.Duration(rand.Intn(4)*50) * time.Millisecond)
				if err := ts.h.GoStart(); err != nil {
					errC <- errors.Wrap(err, "startup failed")
				}
			}
		}()

		var wrote int32
		failed := make([][]byte, 0)
		mu := sync.Mutex{}
		for _, w := range ts.writers {
			// s := ts.scanners[n]
			wg.Add(1)

			go func(w *logd.Writer) {
				defer wg.Done()
				for i := 0; i < n; i++ {
					time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)

					msg := []byte("oh dang sup")
					_, err := w.Write(msg)
					if err != nil {
						if !logd.IsRetryable(err) {
							errC <- errors.Wrap(err, "write failed")
							return
						}

						mu.Lock()
						failed = append(failed, msg)
						mu.Unlock()
						continue
					}
					atomic.AddInt32(&wrote, 1)
				}

				if err := w.Flush(); err != nil && !logd.IsRetryable(err) {
					errC <- errors.Wrap(err, "flush failed")
					return
				}
			}(w)
		}

		wg.Wait()
		failOnErrors(t, errC)

		c, err := logd.DialConfig(ts.cconf.Hostport, ts.cconf)
		if err != nil {
			t.Fatalf("failed to connect for recovering batches: %+v", err)
		}
		for _, batch := range ts.failed {
			if _, err := c.Batch(batch); err != nil {
				t.Fatalf("failed to write previously failed batch: %+v", err)
			}
		}
		c.Close()

		w := logd.NewWriter(ts.cconf, "default")
		for _, msg := range failed {
			if _, err := w.Write(msg); err != nil {
				t.Fatalf("failed to write previously failed message: %+v", err)
			}
			atomic.AddInt32(&wrote, 1)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("failed to flush previously failed messages: %+v", err)
		}
		w.Close()

		if atomic.LoadInt32(&wrote) != int32(n*ts.n) {
			t.Fatalf("expected to write %d messages but only wrote %d", n*ts.n, wrote)
		}
	}
}

func failOnErrors(t *testing.T, errC chan error) {
	t.Helper()

	for {
		select {
		case err := <-errC:
			t.Errorf("%+v", err)
		default:
			return
		}
	}
}
