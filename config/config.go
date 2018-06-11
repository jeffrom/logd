package config

import (
	"encoding/json"
)

// Config holds configuration variables
type Config struct {
	Verbose                 bool   `json:"verbose"`
	CanShutdown             bool   `json:"can_shutdown"`
	Hostport                string `json:"host"`
	MasterHostport          string `json:"master_host"`
	ServerTimeout           uint   `json:"server_timeout"`
	ClientTimeout           uint   `json:"client_timeout"`
	GracefulShutdownTimeout uint   `json:"graceful_shutdown_timeout"`
	LogFile                 string `json:"log_file"`
	LogFileMode             int    `json:"log_file_mode"`
	MaxBatchSize            int    `json:"max_batch_size"`
	PartitionSize           int    `json:"partition_size"`
	MaxPartitions           int    `json:"max_partitions"`
	PartitionDeleteHook     string `json:"partition_delete_hook"`
	IndexCursorSize         uint64 `json:"index_cursor_size"`

	// client configs
	StartID             uint64 `json:"start"`
	ReadLimit           uint64 `json:"limit"`
	ReadForever         bool   `json:"forever"`
	ReadFromTail        bool   `json:"from_tail"`
	ClientBatchSize     int    `json:"client_batch_size"`
	ClientBatchInterval int    `json:"client_batch_interval"`
	ClientWaitInterval  int    `json:"client_wait_interval"`
}

// NewConfig returns a new configuration object
func NewConfig() *Config {
	return &Config{}
}

func (c *Config) String() string {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		b = append(b, []byte(err.Error())...)
	}
	return string(b)
}

// DefaultConfig is the default application config
var DefaultConfig *Config

func init() {
	DefaultConfig = NewConfig()
	DefaultConfig.ServerTimeout = 1000
	DefaultConfig.GracefulShutdownTimeout = 1000
	DefaultConfig.LogFile = "__"
	DefaultConfig.LogFileMode = 0644
	DefaultConfig.MaxBatchSize = 1024 * 20
	// DefaultConfig.MaxBatchSize = 1024 * 1024 * 2
	DefaultConfig.PartitionSize = 1024 * 50
	// DefaultConfig.PartitionSize = 1024 * 1024 * 2000
	DefaultConfig.MaxPartitions = 5
	// DefaultConfig.IndexCursorSize = 1000
	DefaultConfig.IndexCursorSize = 10

	DefaultConfig.ClientTimeout = 1000
	DefaultConfig.ClientWaitInterval = 500
	DefaultConfig.ReadLimit = 15
	DefaultConfig.ReadFromTail = false
	DefaultConfig.ClientBatchSize = 100
	DefaultConfig.ClientBatchInterval = 500

	// XXX just for dev
	DefaultConfig.CanShutdown = true
}

// IndexFileName returns the path of the index file
func (c *Config) IndexFileName() string {
	return c.LogFile + ".index"
}
