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
	GracefulShutdownTimeout uint   `json:"graceful_shutdown_timeout"`
	LogFile                 string `json:"log_file"`
	LogFileMode             int    `json:"log_file_mode"`
	MaxBatchSize            int    `json:"max_batch_size"`
	PartitionSize           int    `json:"partition_size"`
	MaxPartitions           int    `json:"max_partitions"`
	PartitionDeleteHook     string `json:"partition_delete_hook"`
}

// New returns a new configuration object
func New() *Config {
	return &Config{}
}

func (c *Config) String() string {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		b = append(b, []byte(err.Error())...)
	}
	return string(b)
}

// Default is the default application config
var Default *Config

func init() {
	Default = New()
	Default.ServerTimeout = 1000
	Default.GracefulShutdownTimeout = 1000
	Default.LogFile = "__"
	Default.LogFileMode = 0644
	Default.MaxBatchSize = 1024 * 20
	// Default.MaxBatchSize = 1024 * 1024 * 2
	// Default.PartitionSize = 1024 * 1024
	Default.PartitionSize = 1024 * 1024 * 2000
	// Default.MaxPartitions = 5
	Default.MaxPartitions = 8
	// Default.IndexCursorSize = 1000

	// XXX just for dev
	Default.CanShutdown = true
}

// IndexFileName returns the path of the index file
func (c *Config) IndexFileName() string {
	return c.LogFile + ".index"
}
