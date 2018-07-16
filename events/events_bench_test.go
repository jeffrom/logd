package events

import (
	"io/ioutil"
	"log"
	"runtime/debug"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func eventQBenchConfig() *config.Config {
	config := config.New()
	config.ServerTimeout = 500
	config.GracefulShutdownTimeout = 500
	config.MaxBatchSize = 1024 * 10
	config.PartitionSize = 2048
	config.MaxPartitions = 5

	config.Verbose = false

	return config
}

func startQForBench(b *testing.B) (*EventQ, func()) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.WorkDir = testhelper.TmpLog()

	q := NewEventQ(config)
	if err := q.GoStart(); err != nil {
		b.Logf("%s", debug.Stack())
		b.Fatalf("error starting queue: %v", err)
	}
	return q, nil
}

func BenchmarkEventQLifecycle(b *testing.B) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.WorkDir = testhelper.TmpLog()

	for i := 0; i < b.N; i++ {
		q := NewEventQ(config)
		q.GoStart()
		q.handleShutdown()
		q.Stop()
	}
}
