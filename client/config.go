package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/jeffrom/logd/config"
)

// Config is used for client configuration
type Config struct {
	// shared options
	Verbose              bool          `json:"verbose"`
	Hostport             string        `json:"host"`
	Timeout              time.Duration `json:"timeout"`
	WriteTimeout         time.Duration `json:"write-timeout"`
	ReadTimeout          time.Duration `json:"read-timeout"`
	Count                bool          `json:"count"`
	OutputPath           string        `json:"output"`
	WaitInterval         time.Duration `json:"wait-interval"`
	ConnRetries          int           `json:"connection-retries"`
	ConnRetryInterval    time.Duration `json:"connection-retry-interval"`
	ConnRetryMaxInterval time.Duration `json:"connection-retry-max-interval"`
	ConnRetryMultiplier  float64       `json:"connection-retry-multiplier"`

	// write options
	BatchSize    int    `json:"batch-size"`
	WriteForever bool   `json:"write-forever"`
	InputPath    string `json:"input"`

	// read options
	Limit       int    `json:"limit"`
	Offset      uint64 `json:"offset"`
	ReadForever bool   `json:"read-forever"`
	UseTail     bool   `json:"use-tail"`
}

// DefaultConfig is the default client configuration
var DefaultConfig = &Config{
	Verbose:              false,
	Hostport:             "127.0.0.1:1774",
	Timeout:              10 * time.Second,
	WriteTimeout:         -1,
	ReadTimeout:          -1,
	Count:                false,
	OutputPath:           "-",
	WaitInterval:         400 * time.Millisecond,
	ConnRetries:          50,
	ConnRetryInterval:    1 * time.Second,
	ConnRetryMaxInterval: 30 * time.Second,
	ConnRetryMultiplier:  2.0,

	BatchSize: 1024 * 64,
	InputPath: "-",

	Limit: 15,
}

// Validate returns an error pointing to incorrect values for the
// configuration, if any.
func (c *Config) Validate() error {
	if c.ConnRetryMultiplier < 1.0 {
		return errors.New("conn-retry-multiplier must be >= 1.0")
	}
	return nil
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

// ToGeneralConfig copies what is needed for shared modules (internal,
// protocol) to the server config struct.
func (c *Config) ToGeneralConfig() *config.Config {
	gconf := &config.Config{}
	*gconf = *config.Default
	gconf.Verbose = c.Verbose
	gconf.Host = c.Hostport
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
	c.BatchSize = 1024 * 20
	c.ReadTimeout = 100 * time.Millisecond
	c.WriteTimeout = 100 * time.Millisecond
	c.WaitInterval = -1
	c.ConnRetries = 3
	c.ConnRetryInterval = 1
	c.ConnRetryMaxInterval = 1
	c.ConnRetryMultiplier = 2
	return c
}
