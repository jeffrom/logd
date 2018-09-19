package events

import (
	"fmt"
	"sync"
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

	c.ConnRetryInterval /= 2
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
	for _, w := range ts.writers {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}
	for _, s := range ts.scanners {
		if err := s.Close(); err != nil {
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

func testIntegration(t *testing.T, ts *integrationTest) {
	ts.setup(t)
	// defer ts.shutdown(t)

	testIntegrationWriter(t, ts)
}

func testIntegrationWriter(t *testing.T, ts *integrationTest) {
	errC := make(chan error, ts.n)
	wg := sync.WaitGroup{}
	for _, w := range ts.writers {
		// s := ts.scanners[n]
		wg.Add(1)

		go func(w *client.Writer) {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				_, err := w.Write([]byte("oh dang sup"))
				if err != nil {
					errC <- errors.Wrap(err, "write failed")
					return
				}
			}

			if err := w.Flush(); err != nil {
				errC <- errors.Wrap(err, "flush failed")
				return
			}

		}(w)
	}

	wg.Wait()
	failOnErrors(t, errC)
	for _, w := range ts.writers {
		w.Close()
	}
	// ts.h.h["default"].topic.logw.Flush()

	if err := ts.h.Stop(); err != nil {
		errC <- errors.Wrap(err, "shutdown failed")
		return
	}
	if err := ts.h.GoStart(); err != nil {
		errC <- errors.Wrap(err, "restart failed")
		return
	}

	for _, s := range ts.scanners {
		wg.Add(1)

		go func(s *client.Scanner) {
			defer wg.Done()

			s.Reset()
			s.UseTail()
			passed := false
			for s.Scan() {
				passed = true
				fmt.Println(s.Message().String())
			}
			if err := s.Error(); err != nil {
				errC <- errors.Wrap(err, "scan failed")
				return
			}

			if !passed {
				errC <- errors.New("didn't scan anything")
				return
			}
		}(s)
	}

	wg.Wait()
	failOnErrors(t, errC)
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
