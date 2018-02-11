package config

import (
	"encoding/json"
)

// Config holds configuration variables
type Config struct {
	Verbose             bool   `json:"verbose"`
	CanShutdown         bool   `json:"can_shutdown"`
	Hostport            string `json:"host"`
	ServerTimeout       uint   `json:"server_timeout"`
	ClientTimeout       uint   `json:"client_timeout"`
	LogFile             string `json:"log_file"`
	LogFileMode         int    `json:"log_file_mode"`
	MaxChunkSize        int    `json:"max_chunk_size"`
	PartitionSize       int    `json:"partition_size"`
	MaxPartitions       int    `json:"max_partitions"`
	PartitionDeleteHook string `json:"partition_delete_hook"`
	IndexCursorSize     uint64 `json:"index_cursor_size"`

	// client configs
	StartID     uint64 `json:"start"`
	ReadLimit   uint64 `json:"limit"`
	ReadForever bool   `json:"forever"`
}

// NewConfig returns a new configuration object
func NewConfig() *Config {
	return &Config{}
}

func (c *Config) String() string {
	b, err := json.Marshal(c)
	if err != nil {
		b = append(b, []byte(err.Error())...)
	}
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
	// DefaultConfig.PartitionSize = 1024 * 1024 * 2000
	DefaultConfig.MaxPartitions = 5
	// DefaultConfig.IndexCursorSize = 1000
	DefaultConfig.IndexCursorSize = 10

	DefaultConfig.ReadLimit = 15

	// XXX just for dev
	DefaultConfig.CanShutdown = true
}

func (c *Config) IndexFileName() string {
	return c.LogFile + ".index"
}
