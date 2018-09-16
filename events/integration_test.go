package events

import (
	"sync"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

type integrationTest struct {
	conf  *config.Config
	cconf *client.Config
	offs  []uint64
	mu    sync.Mutex

	h        *Handlers
	writers  []*client.Writer
	scanners []*client.Scanner
}

func newIntegrationTestState(conf *config.Config, cconf *client.Config) *integrationTest {
	return &integrationTest{
		conf:     conf,
		cconf:    cconf,
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

func (ts *integrationTest) setup(t *testing.T, n int) {
	if ts.conf.MaxBatchSize > ts.cconf.BatchSize {
		ts.cconf.BatchSize = ts.conf.MaxBatchSize
	} else {
		ts.conf.MaxBatchSize = ts.cconf.BatchSize
	}

	ts.conf.Host = ":0"
	ts.h = NewHandlers(ts.conf)
	doStartHandler(t, ts.h)

	ts.cconf.Hostport = ts.h.servers[0].ListenAddr().String()
	for i := 0; i < n; i++ {
		w := client.NewWriter(ts.cconf, "default")
		w.WithStateHandler(ts)
		ts.writers = append(ts.writers, w)
		ts.scanners = append(ts.scanners, client.NewScanner(ts.cconf, "default"))
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
	conf := testhelper.DefaultConfig(testing.Verbose())
	cconf := client.DefaultTestConfig(testing.Verbose())
	t.Log("server: ", conf)
	t.Log("client: ", cconf)

	ts := newIntegrationTestState(conf, cconf)
	ts.setup(t, 1)
	defer ts.shutdown(t)

	errC := make(chan error, 1)
	wg := sync.WaitGroup{}
	for _, w := range ts.writers {
		wg.Add(1)

		go func(w *client.Writer) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, err := w.Write([]byte("oh dang sup"))
				if err != nil {
					errC <- err
					return
				}
			}

			if err := w.Flush(); err != nil {
				errC <- err
				return
			}
		}(w)
	}

	wg.Wait()

	for {
		select {
		case err := <-errC:
			t.Error(err)
		default:
			return
		}
	}
}
