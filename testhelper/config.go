package testhelper

import (
	"log"
	"os"
	"time"

	"github.com/jeffrom/logd/config"
)

func DefaultTestConfig(verbose bool) *config.Config {
	if verbose {
		log.SetOutput(os.Stdout)
	}

	return &config.Config{
		Verbose:         verbose,
		Timeout:         200 * time.Millisecond,
		ShutdownTimeout: 1 * time.Second,
		LogFileMode:     0644,
		WorkDir:         TmpLog(),
		MaxBatchSize:    1024 * 2,
		PartitionSize:   1024 * 5,
		MaxPartitions:   5,
	}
}

func TestConfig(verbose bool) *config.Config {
	conf := config.New()
	conf.Timeout = 1 * time.Second
	conf.ShutdownTimeout = 1 * time.Second
	conf.MaxBatchSize = 1024 * 10
	conf.PartitionSize = 1024 * 1024 * 500
	conf.WorkDir = TmpLog()
	conf.LogFileMode = 0644
	conf.MaxPartitions = 5

	conf.Verbose = verbose

	return conf
}
