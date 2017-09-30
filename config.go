package logd

import "encoding/json"

// Config holds configuration variables
type Config struct {
	Verbose         bool
	CanShutdown     bool
	Logger          Logger
	Hostport        string
	ServerTimeout   uint
	ClientTimeout   uint
	LogFile         string
	LogFileMode     int
	MaxChunkSize    int
	PartitionSize   int
	IndexCursorSize uint64
	StartID         uint64
	ReadLimit       uint64
	ReadForever     bool
}

// NewConfig returns a new configuration object
func NewConfig() *Config {
	return &Config{}
}

func (c *Config) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}

// DefaultConfig is the default application config
var DefaultConfig *Config

func init() {
	DefaultConfig = NewConfig()
	DefaultConfig.ServerTimeout = 500
	DefaultConfig.ClientTimeout = 500
	DefaultConfig.LogFile = "__log"
	DefaultConfig.LogFileMode = 0644
	DefaultConfig.MaxChunkSize = 1024 * 1024 * 2
	DefaultConfig.PartitionSize = 1024 * 50
	// DefaultConfig.PartitionSize = 1024 * 50
	DefaultConfig.IndexCursorSize = 1000
	// DefaultConfig.IndexCursorSize = 100

	DefaultConfig.ReadLimit = 15

	// XXX just for dev
	DefaultConfig.CanShutdown = true

	// logger := newFileLogger(DefaultConfig)
	// logger.discard = true
	// DefaultConfig.Logger = logger
}

func (c *Config) indexFileName() string {
	return c.LogFile + ".index"
}
