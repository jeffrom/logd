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

	// write options
	BatchSize    int           `json:"batch-size"`
	Limit        int           `json:"limit"`
	WaitInterval time.Duration `json:"wait-interval"`
	WriteForever bool          `json:"write-forever"`
	InputPath    string        `json:"input"`
	OutputPath   string        `json:"output"`
}

// DefaultConfig is the default client configuration
var DefaultConfig = &Config{
	Verbose:      false,
	Hostport:     "127.0.0.1:1774",
	Timeout:      500 * time.Millisecond,
	WriteTimeout: -1 * time.Millisecond,
	ReadTimeout:  -1 * time.Millisecond,
	Count:        false,

	BatchSize:    1024 * 100,
	Limit:        500,
	WaitInterval: 1000 * time.Millisecond,
	InputPath:    "-",
	OutputPath:   "-",
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
	*gconf = *config.DefaultConfig
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
