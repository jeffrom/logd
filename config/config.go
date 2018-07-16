package config

import (
	"encoding/json"
)

// Config holds configuration variables
type Config struct {
	Verbose                 bool   `json:"verbose"`
	CanShutdown             bool   `json:"can-shutdown"`
	Hostport                string `json:"host"`
	ServerTimeout           uint   `json:"server-timeout"`
	GracefulShutdownTimeout uint   `json:"graceful-shutdown-timeout"`
	WorkDir                 string `json:"work-dir"`
	LogFileMode             int    `json:"log-file-mode"`
	MaxBatchSize            int    `json:"max-batch-size"`
	PartitionSize           int    `json:"partition-size"`
	MaxPartitions           int    `json:"max-partitions"`
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
var Default = &Config{
	ServerTimeout:           1000,
	GracefulShutdownTimeout: 1000,
	WorkDir:                 "logs/",
	LogFileMode:             0600,
	MaxBatchSize:            1024 * 64,
	PartitionSize:           1024 * 1024 * 2000,
	MaxPartitions:           8,
}
