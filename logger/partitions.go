package logger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jeffrom/logd/config"
)

// ErrNotFound is returned when a partition could not be found
var ErrNotFound = errors.New("partition not found")

// PartitionManager gets, create, and otherwise manages partitions
type PartitionManager interface {
	// Create creates a new partition. If the partition already exists, return
	// an error
	Create(off uint64) (Partitioner, error)
	// Uncirculate makes a partition unavailable to subsequent requests.
	// Intended for allowing current reads to complete before permanent
	// removal.
	Uncirculate(off uint64) error
	// Remove removes a partition. If the partition doesn't exist, return an
	// error
	Remove(off uint64) error
	// Get returns an io.Reader representing the partition located at offset,
	// its start position seeked to delta, and limited to reading limit bytes
	Get(offset uint64, delta int64, limit int64) (io.Reader, error)
	// List returns a list of the currently available partition offsets
	List() ([]Partitioner, error)
}

// Partitioner wraps the log partition. in most usage, an *os.File
type Partitioner interface {
	Offset() uint64
	Size() int
	Reader() io.ReadCloser
}

// Partitions implements PartitionManager. It creates, removes, lists, and gets
// Partitions to be read from by server connections.
type Partitions struct {
	conf       *config.Config
	partitions []Partitioner
}

// NewPartitions returns an instance of Partitions
func NewPartitions(conf *config.Config) *Partitions {
	return &Partitions{
		conf:       conf,
		partitions: make([]Partitioner, conf.MaxPartitions),
	}
}

// Create implements PartitionManager. TODO may not be needed. Writer can create them.
func (p *Partitions) Create(off uint64) (Partitioner, error) {
	return nil, nil
}

// Uncirculate implements PartitionManager
func (p *Partitions) Uncirculate(off uint64) error {
	return nil
}

// Remove implements PartitionManager
func (p *Partitions) Remove(off uint64) error {
	return nil
}

// Get implements PartitionManager
func (p *Partitions) Get(off uint64, delta int64, limit int64) (io.Reader, error) {
	return nil, nil
}

// List implements PartitionManager
func (p *Partitions) List() ([]Partitioner, error) {
	pat := p.conf.LogFile + "[0-9]*.log"
	matches, err := filepath.Glob(pat)
	if err != nil {
		return nil, err
	}
	var parts []Partitioner
	for _, match := range matches {
		info, serr := os.Stat(match)
		if serr != nil {
			return nil, serr
		}

		off, perr := p.extractOffset(match)
		if perr != nil {
			return nil, perr
		}

		part := NewPartition(p.conf, off, int(info.Size()))
		parts = append(parts, part)
	}

	p.partitions = parts
	sort.Sort(p)

	return parts, nil
}

func (p *Partitions) extractOffset(filename string) (uint64, error) {
	s := strings.TrimPrefix(filename, p.conf.LogFile)
	s = strings.TrimSuffix(s, ".log")
	return strconv.ParseUint(s, 10, 64)
}

// Len implements sort.Interface
func (p *Partitions) Len() int { return len(p.partitions) }

// Swap implements sort.Interface
func (p *Partitions) Swap(i, j int) {
	p.partitions[i], p.partitions[j] = p.partitions[j], p.partitions[i]
}

// Less implements sort.Interface
func (p *Partitions) Less(i, j int) bool {
	return p.partitions[i].Offset() < p.partitions[j].Offset()
}

// func (p *Partitions) Create(off uint64) (*Partition, error)

// Partition implements Partitioner
type Partition struct {
	conf   *config.Config
	offset uint64
	size   int
	reader io.ReadCloser
}

// NewPartition returns a new instance of Partition
func NewPartition(conf *config.Config, offset uint64, size int) *Partition {
	return &Partition{
		conf:   conf,
		offset: offset,
		size:   size,
	}
}

// Reset sets Partition to its initial params
func (p *Partition) Reset() {
	p.offset = 0
	p.size = 0
}

func (p *Partition) String() string {
	return fmt.Sprintf("logger.Partition<offset: %d, size: %d>", p.offset, p.size)
}

// Offset implements Partitioner
func (p *Partition) Offset() uint64 {
	return p.offset
}

// Reader implements Partitioner
func (p *Partition) Reader() io.ReadCloser {
	return nil
}

// Size implements Partitioner
func (p *Partition) Size() int {
	return p.size
}

// PartitionFile wraps a LimitReader(*os.File) so it can be unwrapped by the
// socket and sendfile can be leveraged.
// TODO PartitionFile should also be involved in deletions. each partition on
// disk should have an associated reference count, and when it's slated for
// deletion, it should wait until all references have been closed before
// deleting the file from disk. It should move the file into a temp directory
// so subsequent requests for deleted offsets return not found errors.
type PartitionFile struct {
	conf *config.Config
}
