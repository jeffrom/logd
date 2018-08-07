package testhelper

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
)

func DefaultTestConfig(verbose bool) *config.Config {
	if verbose {
		log.SetOutput(os.Stdout)
	}

	c := &config.Config{
		Verbose:         verbose,
		Timeout:         200 * time.Millisecond,
		ShutdownTimeout: 1 * time.Second,
		LogFileMode:     0644,
		WorkDir:         TmpLog(),
		MaxBatchSize:    1024 * 2,
		PartitionSize:   1024 * 5,
		MaxPartitions:   5,
	}

	if !testing.Short() && IsCI() {
		c.Timeout = 2 * time.Second
		c.ShutdownTimeout = 5 * time.Second
	}

	return c
}
