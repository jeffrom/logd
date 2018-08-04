package logger

import (
	"io"
	"os"

	"github.com/jeffrom/logd/config"
)

// LogRepairer truncates corrupted data
type LogRepairer interface {
	Truncate(part uint64, size int64) error
	Data(part uint64) (io.ReadCloser, error)
}

// Repairer implements LogRepairer using the filesystem
type Repairer struct {
	conf  *config.Config
	topic string
}

// NewRepairer returns a new instance of *Repairer
func NewRepairer(conf *config.Config, topic string) *Repairer {
	return &Repairer{
		conf:  conf,
		topic: topic,
	}
}

// Truncate implements LogRepairer interface
func (r *Repairer) Truncate(part uint64, size int64) error {
	p := partitionFullPath(r.conf, r.topic, part)
	return os.Truncate(p, size)
}

// Data implements LogRepairer interface
func (r *Repairer) Data(part uint64) (io.ReadCloser, error) {
	p := partitionFullPath(r.conf, r.topic, part)
	return os.Open(p)
}
