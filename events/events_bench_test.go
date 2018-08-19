package events

import (
	"io/ioutil"
	"log"
	"runtime/debug"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func eventHandlerBenchConfig() *config.Config {
	config := config.New()
	config.Timeout = 500 * time.Millisecond
	config.ShutdownTimeout = 500 * time.Millisecond
	config.MaxBatchSize = 1024 * 10
	config.PartitionSize = 2048
	config.MaxPartitions = 5

	config.Verbose = false

	return config
}

func startHandlerForBench(b *testing.B) (*Handlers, func()) {
	config := eventHandlerBenchConfig()
	config.LogFileMode = 0644
	config.WorkDir = testhelper.TmpLog()

	h := NewHandlers(config)
	if err := h.GoStart(); err != nil {
		b.Logf("%s", debug.Stack())
		b.Fatalf("error starting queue: %v", err)
	}
	return h, nil
}

func BenchmarkLifecycle(b *testing.B) {
	config := eventHandlerBenchConfig()
	config.LogFileMode = 0644
	config.WorkDir = testhelper.TmpLog()

	for i := 0; i < b.N; i++ {
		h := NewHandlers(config)
		h.GoStart()
		h.Stop()
	}
}
