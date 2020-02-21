package logd

import (
	"errors"
	"fmt"
	"time"

	"github.com/jeffrom/logd/config"
)

// Config is used for client configuration
type Config struct {
	// shared options

	// Verbose prints debugging information.
	Verbose bool `json:"verbose"`

	// Host defines the host:port to connect to.
	Host string `json:"host"`

	// Timeout defines the overall socket timeout. For more granular control,
	// set ConnectTimeout, WriteTimeout, and ReadTimeout.
	Timeout time.Duration `json:"timeout"`

	// ConnectTimeout defines the time limit for connecting to the server.
	ConnectTimeout time.Duration `json:"connect-timeout"`

	// WriteTimeout defines the time limit for writing to the server socket.
	WriteTimeout time.Duration `json:"write-timeout"`

	// ReadTimeout defines the time limit for reading from the server socket.
	ReadTimeout time.Duration `json:"read-timeout"`

	// Count prints some counts of messages written before exiting log-cli.
	Count bool `json:"count"`

	// OutputPath is the file log-cli will write output data to. Defaults to
	// standard out.
	OutputPath string `json:"output"`

	// WaitInterval, for Writer, determines the length of time, without any
	// messages written, before the batch is flushed to the server. For
	// Scanner, determines how long to wait before requesting new batches.
	// TODO these should be split out when writer and scanner have seperate
	// configs.
	WaitInterval time.Duration `json:"wait-interval"`

	// ConnRetries defines how many attempts to connect should happen. A
	// negative number will retry forever.
	ConnRetries int `json:"connection-retries"`

	// ConnRetryMaxInterval defines the initial amount of time to wait before
	// attempting to reconnect.
	ConnRetryInterval time.Duration `json:"connection-retry-interval"`

	// ConnRetryMaxInterval defines the maximum amount of time to wait before
	// attempting to reconnect.
	ConnRetryMaxInterval time.Duration `json:"connection-retry-max-interval"`

	// ConnRetryMultiplier defines the multiplier for reconnection attempts.
	// For example, if the multiplier is 2.0, the amount of time to wait will
	// double each time a reconnect is attempted, up to ConnRetryMaxInterval.
	ConnRetryMultiplier float64 `json:"connection-retry-multiplier"`

	// write options

	// BatchSize is the maximum batch size to reach before flushing a batch to
	// the server.
	BatchSize int `json:"batch-size"`

	// InputPath defines the file from which log-cli reads messages. Defaults
	// to standard input.
	InputPath string `json:"input"`

	// read options

	// Limit defines the minimum number of messages to read per read operation.
	Limit int `json:"limit"`

	// Offset defines the initial offset from which to read messages.
	Offset uint64 `json:"offset"`

	// ReadForever will read messages until the scanner is stopped.
	ReadForever bool `json:"read-forever"`

	// UseTail tells the scanner to begin reading from the beginning of the
	// log.
	UseTail bool `json:"use-tail"`
}

// DefaultConfig is the default client configuration
var DefaultConfig = &Config{
	Verbose:              false,
	Host:                 "127.0.0.1:1774",
	Timeout:              10 * time.Second,
	ConnectTimeout:       -1,
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

// NewConfig returns a new default client configuration.
func NewConfig() *Config {
	conf := &Config{}
	*conf = *DefaultConfig
	return conf
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

func (c *Config) getConnectTimeout() time.Duration {
	if c.ConnectTimeout >= 0 {
		return c.ConnectTimeout
	}
	return c.Timeout
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
	gconf.Host = c.Host
	gconf.MaxBatchSize = c.BatchSize
	return gconf
}

// FromGeneralConfig returns a new client config that copies common attributes.
func (c *Config) FromGeneralConfig(conf *config.Config) *Config {
	newc := &Config{}
	*newc = *c

	c.Verbose = conf.Verbose
	c.Host = conf.Host
	c.BatchSize = conf.MaxBatchSize

	return newc
}

// BenchConfig manages benchmark configuration
type BenchConfig struct {
	Verbose bool
}

// DefaultBenchConfig is the default benchmarking configuration
var DefaultBenchConfig = &BenchConfig{
	Verbose: false,
}

var cachedPort = 4771

// DefaultTestConfig returns a testing configuration
func DefaultTestConfig(verbose bool) *Config {
	c := &Config{}
	*c = *DefaultConfig
	c.Host = fmt.Sprintf("127.0.0.1:%d", cachedPort)
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
