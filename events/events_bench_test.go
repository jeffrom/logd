package events

import (
	"io/ioutil"
	"log"
	"runtime/debug"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func eventQBenchConfig() *config.Config {
	config := config.NewConfig()
	config.ServerTimeout = 500
	config.ClientTimeout = 500
	config.GracefulShutdownTimeout = 500
	config.MaxBatchSize = 1024 * 10
	config.IndexCursorSize = 1000
	config.PartitionSize = 2048
	config.MaxPartitions = 5

	config.Verbose = false

	return config
}

func startQForBench(b *testing.B) (*EventQ, func()) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.LogFile = testhelper.TmpLog()

	q := NewEventQ(config)
	if err := q.Start(); err != nil {
		b.Logf("%s", debug.Stack())
		b.Fatalf("error starting queue: %v", err)
	}
	return q, nil
}

func BenchmarkEventQLifecycle(b *testing.B) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.LogFile = testhelper.TmpLog()

	for i := 0; i < b.N; i++ {
		q := NewEventQ(config)
		q.Start()
		q.handleShutdown()
		q.Stop()
	}
}

func drainReaderChan(readerC chan protocol.ReadPart) {
	n := 0
	for {
		select {
		case <-readerC:
			n++
		case <-time.After(time.Millisecond * 500):
			panic("timed out waiting for messages on readerC")
		default:
			if n > 0 {
				return
			}
		}
	}
}
