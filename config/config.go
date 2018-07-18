package config

import (
	"time"
)

// Config holds configuration variables
type Config struct {
	File            string        `json:"config-file"`
	Verbose         bool          `json:"verbose"`
	CanShutdown     bool          `json:"can-shutdown"`
	Hostport        string        `json:"host"`
	Timeout         time.Duration `json:"timeout"`
	ShutdownTimeout time.Duration `json:"shutdown-timeout"`
	WorkDir         string        `json:"work-dir"`
	LogFileMode     int           `json:"log-file-mode"`
	MaxBatchSize    int           `json:"max-batch-size"`
	PartitionSize   int           `json:"partition-size"`
	MaxPartitions   int           `json:"max-partitions"`
}

// New returns a new configuration object
func New() *Config {
	return &Config{}
}

// func (c *Config) String() string {
// 	b, err := json.MarshalIndent(c, "", "  ")
// 	if err != nil {
// 		b = append(b, []byte(err.Error())...)
// 	}
// 	return string(b)
// }

// Default is the default application config
var Default = &Config{
	Hostport:        "localhost:1774",
	Timeout:         1 * time.Second,
	ShutdownTimeout: 1 * time.Second,
	WorkDir:         "logs/",
	LogFileMode:     0600,
	MaxBatchSize:    1024 * 64,
	PartitionSize:   1024 * 1024 * 2000,
	MaxPartitions:   8,
}
