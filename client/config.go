package client

import (
	"fmt"
	"time"

	"github.com/jeffrom/logd/config"
)

// Config is used for client configuration
type Config struct {
	// shared options
	Verbose      bool          `json:"verbose"`
	Hostport     string        `json:"host"`
	Timeout      time.Duration `json:"timeout"`
	WriteTimeout time.Duration `json:"write-timeout"`
	ReadTimeout  time.Duration `json:"read-timeout"`
	Count        bool          `json:"count"`
	OutputPath   string        `json:"output"`
	WaitInterval time.Duration `json:"wait-interval"`

	// write options
	BatchSize    int    `json:"batch-size"`
	WriteForever bool   `json:"write-forever"`
	InputPath    string `json:"input"`

	// read options
	Limit       int    `json:"limit"`
	Offset      uint64 `json:"offset"`
	ReadForever bool   `json:"read-forever"`
}

// DefaultConfig is the default client configuration
var DefaultConfig = &Config{
	Verbose:      false,
	Hostport:     "127.0.0.1:1774",
	Timeout:      500 * time.Millisecond,
	WriteTimeout: -1 * time.Millisecond,
	ReadTimeout:  -1 * time.Millisecond,
	Count:        false,
	OutputPath:   "-",
	WaitInterval: 400 * time.Millisecond,

	BatchSize: 1024 * 20,
	InputPath: "-",

	Limit: 15,
}

func (c *Config) String() string {
	return fmt.Sprintf("%+v", *c)
}

func (c *Config) getReadTimeout() time.Duration {
	if c.ReadTimeout >= 0 {
		return c.ReadTimeout
	}
	return c.Timeout
}

func (c *Config) getWriteTimeout() time.Duration {
	if c.WriteTimeout >= 0 {
		return c.WriteTimeout
	}
	return c.Timeout
}

// just copies what is needed for shared modules (internal, protocol)
func (c *Config) toGeneralConfig() *config.Config {
	gconf := &config.Config{}
	*gconf = *config.Default
	gconf.Verbose = c.Verbose
	gconf.Hostport = c.Hostport
	gconf.MaxBatchSize = c.BatchSize
	return gconf
}

// BenchConfig manages benchmark configuration
type BenchConfig struct {
	Verbose bool
}

// DefaultBenchConfig is the default benchmarking configuration
var DefaultBenchConfig = &BenchConfig{
	Verbose: false,
}

// DefaultTestConfig returns a testing configuration
func DefaultTestConfig(verbose bool) *Config {
	c := &Config{}
	*c = *DefaultConfig
	c.Verbose = verbose
	// c.BatchSize = 1024 * 10
	return c
}
