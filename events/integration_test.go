package events

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
	"github.com/pkg/errors"
)

func newIntegrationTestClientConfig(verbose bool) *client.Config {
	c := client.NewConfig()
	*c = *client.DefaultConfig

	c.Verbose = verbose
	c.BatchSize = 1024 * 20

	c.ConnRetryInterval /= 4
	c.ConnRetries = 3
	return c
}

type integrationTest struct {
	conf  *config.Config
	cconf *client.Config
	offs  []uint64
	mu    sync.Mutex

	n        int
	h        *Handlers
	writers  []*client.Writer
	scanners []*client.Scanner
}

func newIntegrationTestState(conf *config.Config, cconf *client.Config, n int) *integrationTest {
	return &integrationTest{
		conf:     conf,
		cconf:    cconf,
		n:        n,
		offs:     []uint64{},
		writers:  make([]*client.Writer, 0),
		scanners: make([]*client.Scanner, 0),
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
	ts.h = NewHandlers(ts.conf)
	doStartHandler(t, ts.h)

	ts.cconf.Hostport = ts.h.servers[0].ListenAddr().String()
	for i := 0; i < ts.n; i++ {
		w := client.NewWriter(ts.cconf, "default")
		w.WithStateHandler(ts)

		s := client.NewScanner(ts.cconf, "default")

		ts.writers = append(ts.writers, w)
		ts.scanners = append(ts.scanners, s)
	}
}

func (ts *integrationTest) shutdown(t *testing.T) {
	defer doShutdownHandler(t, ts.h)
	for _, s := range ts.scanners {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}
	for _, w := range ts.writers {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}
}

// Push implements client.StatePusher.
func (ts *integrationTest) Push(off uint64, err error, batch *protocol.Batch) error {
	if err != nil {
		// TODO add failed batches to the test state
		return err
	}

	ts.addOff(off)

	return nil
}

// Close implements client.StatePusher.
func (ts *integrationTest) Close() error {
	return nil
}

func TestIntegration(t *testing.T) {
	conf := testhelper.IntegrationTestConfig(testing.Verbose())
	cconf := newIntegrationTestClientConfig(testing.Verbose())

	ts := newIntegrationTestState(conf, cconf, 1)
	testIntegration(t, ts)
}

func TestIntegration4(t *testing.T) {
	conf := testhelper.IntegrationTestConfig(testing.Verbose())
	cconf := newIntegrationTestClientConfig(testing.Verbose())

	ts := newIntegrationTestState(conf, cconf, 4)
	testIntegration(t, ts)
}

func testIntegration(t *testing.T, ts *integrationTest) {
	ts.setup(t)
	defer ts.shutdown(t)

	t.Run("write then read", func(t *testing.T) {
		testIntegrationWriter(t, ts)
	})
}

func testIntegrationWriter(t *testing.T, ts *integrationTest) {
	n := 10000
	errC := make(chan error, ts.n)
	wg := sync.WaitGroup{}

	var wrote int32
	for _, w := range ts.writers {
		// s := ts.scanners[n]
		wg.Add(1)

		go func(w *client.Writer) {
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

		go func(s *client.Scanner) {
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

	// if err := ts.h.Stop(); err != nil {
	// 	errC <- errors.Wrap(err, "shutdown failed")
	// 	return
	// }
	// if err := ts.h.GoStart(); err != nil {
	// 	errC <- errors.Wrap(err, "restart failed")
	// 	return
	// }
}

func failOnErrors(t *testing.T, errC chan error) {
	for {
		select {
		case err := <-errC:
			t.Errorf("%+v", err)
		default:
			return
		}
	}
}
