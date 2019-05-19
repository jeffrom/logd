package config

import (
	"fmt"
	"time"
)

// Config holds configuration variables
type Config struct {
	// File is the path of a file from which configuration is read.
	File string `json:"config-file"`

	// Verbose prints debugging information.
	Verbose bool `json:"verbose"`

	// Host is the socket listener host:port.
	Host string `json:"host"`

	// HttpHost is the http listener host:port
	HttpHost string `json:"http-host"`

	// Timeout determines how long to wait during requests before closing the
	// connection if the request hasn't completed.
	Timeout time.Duration `json:"timeout"`

	// IdleTimeout determines how long a connection can live without any
	// requests issued.
	IdleTimeout time.Duration `json:"idle-timeout"`

	// ShutdownTimeout determines how long active connections should be waited
	// on during the shutdown sequence.
	ShutdownTimeout time.Duration `json:"shutdown-timeout"`

	// WorkDir defines the working directory.
	WorkDir string `json:"work-dir"`

	// LogFileMode defines the file permissions for log files.
	LogFileMode int `json:"log-file-mode"`

	// MaxBatchSize defines the maximum allowed size for a batch.
	MaxBatchSize int `json:"max-batch-size"`

	// PartitionSize defines the maximum partition size.
	PartitionSize int `json:"partition-size"`

	// MaxPartitions defines the maximum number of partitions. Once this has
	// been reached, the oldest partition will be rotated out and deleted.
	MaxPartitions int `json:"max-partitions"`

	// FlushBatches defines the number of batches to write before syncing to
	// disk.
	FlushBatches int `json:"flush-batches"`

	// FlushInterval defines the amount of time elapsed before syncing to disk.
	FlushInterval time.Duration `json:"flush-interval"`

	// MaxTopics can be defined to limit the number of allowed topics.
	MaxTopics int `json:"topics"`

	// TopicWhitelist can be defined to limit topics to a predefined list.
	TopicWhitelist []string `json:"topic-whitelist"`
}

// New returns a new configuration object
func New() *Config {
	return &Config{}
}

func (c *Config) String() string {
	return fmt.Sprintf("%+v", *c)
}

// Default is the default application config
var Default = &Config{
	Host:            ":1774",
	HttpHost:        ":1775",
	Timeout:         15 * time.Second,
	IdleTimeout:     30 * time.Second,
	ShutdownTimeout: 15 * time.Second,
	WorkDir:         "logs/",
	LogFileMode:     0600,
	MaxBatchSize:    1024 * 64,
	PartitionSize:   1024 * 1024 * 2000,
	MaxPartitions:   8,
	FlushBatches:    0,
	FlushInterval:   -1,
	MaxTopics:       8,
}
