package testhelper

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
)

func init() {
	if IsCI() {
		fmt.Println("We're in CI!")
	}
}

func DefaultConfig(verbose bool) *config.Config {
	if verbose {
		log.SetOutput(os.Stdout)
	}

	c := &config.Config{
		Verbose:         verbose,
		Timeout:         200 * time.Millisecond,
		IdleTimeout:     200 * time.Millisecond,
		ShutdownTimeout: 1 * time.Second,
		LogFileMode:     0644,
		WorkDir:         TmpLog(),
		MaxBatchSize:    1024 * 2,
		PartitionSize:   1024 * 5,
		MaxPartitions:   5,
	}

	if !testing.Short() && IsCI() {
		c.Timeout = 10 * time.Second
		c.IdleTimeout = 10 * time.Second
		c.ShutdownTimeout = 15 * time.Second
	}

	return c
}

func IntegrationTestConfig(verbose bool) *config.Config {
	if verbose {
		log.SetOutput(os.Stdout)
	}

	c := &config.Config{
		Verbose:         verbose,
		Timeout:         1 * time.Second,
		IdleTimeout:     3 * time.Second,
		ShutdownTimeout: 2 * time.Second,
		LogFileMode:     0644,
		WorkDir:         TmpLog(),
		MaxBatchSize:    1024 * 20,
		PartitionSize:   1024 * 100,
		MaxPartitions:   5,
	}

	if !testing.Short() && IsCI() {
		*c = *config.Default
	}

	return c
}
