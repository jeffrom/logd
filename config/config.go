package config

import (
	"time"
)

// Config holds configuration variables
type Config struct {
	File string `json:"config-file"`

	Verbose     bool   `json:"verbose"`
	CanShutdown bool   `json:"can-shutdown"`
	Host        string `json:"host"`
	HttpHost    string `json:"http-host"`

	// Timeout determines how long to wait during requests before closing the
	// connection if the request hasn't completed.
	Timeout         time.Duration `json:"timeout"`
	IdleTimeout     time.Duration `json:"idle-timeout"`
	ShutdownTimeout time.Duration `json:"shutdown-timeout"`

	WorkDir       string        `json:"work-dir"`
	LogFileMode   int           `json:"log-file-mode"`
	MaxBatchSize  int           `json:"max-batch-size"`
	PartitionSize int           `json:"partition-size"`
	MaxPartitions int           `json:"max-partitions"`
	FlushBatches  int           `json:"flush-batches"`
	FlushInterval time.Duration `json:"flush-interval"`
}

// New returns a new configuration object
func New() *Config {
	return &Config{}
}

// Default is the default application config
var Default = &Config{
	Host:            "localhost:1774",
	HttpHost:        "localhost:1775",
	Timeout:         10 * time.Second,
	IdleTimeout:     30 * time.Second,
	ShutdownTimeout: 15 * time.Second,
	WorkDir:         "logs/",
	LogFileMode:     0600,
	MaxBatchSize:    1024 * 64,
	PartitionSize:   1024 * 1024 * 2000,
	MaxPartitions:   8,
	FlushBatches:    0,
	FlushInterval:   -1,
}
