package logger

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jeffrom/logd/config"
)

// ErrNotFound is returned when a partition could not be found
var ErrNotFound = errors.New("partition not found")

// PartitionManager gets, create, and otherwise manages partitions
type PartitionManager interface {
	// Uncirculate makes a partition unavailable to subsequent requests.
	// Intended for allowing current reads to complete before permanent
	// removal.
	// Uncirculate(off uint64) error
	// Remove removes a partition. If the partition doesn't exist, or
	// Uncirculate was not previously called on the partition, return an error.
	Remove(off uint64) error
	// Get returns an io.Reader representing the partition located at offset,
	// its start position seeked to delta, and limited to limit bytes.
	Get(offset uint64, delta int, limit int) (Partitioner, error)
	// List returns a list of the currently available partition offsets
	List() ([]Partitioner, error)
}

// Partitioner wraps the log partition. in most usage, an *os.File
type Partitioner interface {
	io.ReadCloser
	Offset() uint64
	Size() int
	Reader() io.Reader
}

// Partitions implements PartitionManager. It creates, removes, lists, and gets
// Partitions to be read from by server connections.
type Partitions struct {
	conf       *config.Config
	partitions []Partitioner
	tempDir    string
	refs       map[uint64]int
	mu         sync.Mutex
}

// NewPartitions returns an instance of Partitions
func NewPartitions(conf *config.Config) *Partitions {
	return &Partitions{
		conf:       conf,
		partitions: make([]Partitioner, conf.MaxPartitions),
		refs:       make(map[uint64]int),
	}
}

func (p *Partitions) reset() {
	p.tempDir = ""
}

// Setup implements internal.LifecycleManager
func (p *Partitions) Setup() error {
	return p.ensureTempDir()
}

func (p *Partitions) ensureTempDir() error {
	if p.tempDir == "" {
		// TODO config for tempdir location, file mode
		tpath := "./tmp"
		if strings.HasPrefix(p.conf.LogFile, os.TempDir()) {
			tdir, _ := filepath.Split(p.conf.LogFile)
			tpath = filepath.Join(tdir, "removed")
		}
		dir, perr := filepath.Abs(tpath)
		if perr != nil {
			return perr
		}

		if dir != "" {
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				if err := os.Mkdir(dir, 0700); err != nil {
					return err
				}
			}
		}
		tmpDir, err := ioutil.TempDir(dir, "logd-uncirculated")
		if err != nil {
			return err
		}
		p.tempDir = tmpDir
	}
	return nil
}

// Remove implements PartitionManager
func (p *Partitions) Remove(off uint64) error {
	if err := p.ensureTempDir(); err != nil {
		return err
	}

	fname := partitionPath(p.conf, off)
	if err := os.Rename(partitionFullPath(p.conf, off), filepath.Join(p.tempDir, fname)); err != nil {
		return err
	}

	if p.getRefs(off) <= 0 {
		return p.removeFile(off)
	}
	return nil
}

// Get implements PartitionManager
func (p *Partitions) Get(off uint64, delta int, limit int) (Partitioner, error) {
	fname := p.conf.LogFile + strconv.FormatUint(off, 10) + ".log"
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(fname)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(int64(delta), io.SeekStart); err != nil {
		return nil, err
	}

	size := int(info.Size())
	if limit <= 0 {
		limit = size
	}

	r := NewPartition(p.conf, off, size).withTmpDir(p.tempDir)
	if err := r.setFile(f); err != nil {
		return nil, err
	}
	r.wrapCloser(func(closer io.Closer) error {
		if err := closer.Close(); err != nil {
			log.Printf("error closing %d: %+v", off, err)
		}
		if p.decRefs(off) <= 0 {
			return p.removeFile(off)
		}
		return nil
	})
	p.incRefs(off)
	r.setReader(io.LimitReader(f, int64(limit)))
	return r, nil
}

// List implements PartitionManager
func (p *Partitions) List() ([]Partitioner, error) {
	return p.list(p.conf.LogFile, false)
}

func (p *Partitions) listTempDir() ([]Partitioner, error) {
	_, suf := filepath.Split(p.conf.LogFile)
	return p.list(p.tempDir+"/"+suf, true)
}

func (p *Partitions) list(prefix string, tmp bool) ([]Partitioner, error) {
	pat := prefix + "[0-9]*.log"
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

		off, perr := p.extractOffset(match, tmp)
		if perr != nil {
			return nil, perr
		}

		part := NewPartition(p.conf, off, int(info.Size())).withTmpDir(p.tempDir)
		parts = append(parts, part)
	}

	p.partitions = parts
	sort.Sort(p)

	return parts, nil
}

// Shutdown implements internal.LifecycleManager
func (p *Partitions) Shutdown() error {
	if p.tempDir != "" {
		// TODO log any remaining uncirculated files
		return os.Remove(p.tempDir)
	}
	return nil
}

func (p *Partitions) incRefs(off uint64) {
	p.mu.Lock()
	if _, ok := p.refs[off]; !ok {
		p.refs[off] = 1
	} else {
		p.refs[off]++
	}
	p.mu.Unlock()
}

func (p *Partitions) decRefs(off uint64) int {
	p.mu.Lock()
	p.refs[off]--
	refs := p.refs[off]
	p.mu.Unlock()
	return refs
}

func (p *Partitions) getRefs(off uint64) int {
	p.mu.Lock()
	refs := p.refs[off]
	p.mu.Unlock()
	return refs
}

func (p *Partitions) withCloser(part *Partition) *Partition {
	return part
}

func (p *Partitions) extractOffset(filename string, tmp bool) (uint64, error) {
	logfname := p.conf.LogFile
	if tmp {
		logfname = p.tempDir + "/"
	}
	dir, suf := filepath.Split(logfname)
	if tmp {
		_, suf = filepath.Split(p.conf.LogFile)
	}
	s := strings.TrimPrefix(filename, dir)
	s = strings.TrimPrefix(s, suf)
	s = strings.TrimSuffix(s, ".log")
	return strconv.ParseUint(s, 10, 64)
}

// removeFile deletes the file from the filesystem. Remove must have been called
// first.
func (p *Partitions) removeFile(off uint64) error {
	if p.tempDir == "" {
		return errors.New("Partitions.Remove: temp dir not set")
	}

	ppath := partitionPath(p.conf, off)
	fullpath := filepath.Join(p.tempDir, ppath)
	err := os.Remove(fullpath)
	if err != nil {
		log.Printf("failed to delete %d: %+v", off, err)
	}
	return err
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

//
// Partition
//

// Partition implements Partitioner
type Partition struct {
	conf   *config.Config
	tmpDir string
	offset uint64
	size   int
	reader io.Reader
	closer io.Closer
}

type closeWrap struct {
	closer func() error
}

func closeWrapper(orig io.Closer, f func(io.Closer) error) *closeWrap {
	return &closeWrap{
		closer: func() error {
			return f(orig)
		},
	}
}

func (c *closeWrap) Close() error {
	return c.closer()
}

// NewPartition returns a new instance of Partition
func NewPartition(conf *config.Config, offset uint64, size int) *Partition {
	return &Partition{
		conf:   conf,
		offset: offset,
		size:   size,
	}
}

func (p *Partition) withTmpDir(tmpDir string) *Partition {
	p.tmpDir = tmpDir
	return p
}

func (p *Partition) wrapCloser(f func(io.Closer) error) *Partition {
	orig := p.closer
	p.closer = closeWrapper(orig, f)
	return p
}

// Reset sets Partition to its initial params
func (p *Partition) Reset() {
	p.tmpDir = ""
	p.offset = 0
	p.size = 0
}

func (p *Partition) String() string {
	args := []interface{}{p.offset, p.size}
	s := "logger.Partition<offset: %d, size: %d"
	if f, ok := p.reader.(*os.File); ok {
		s += ", file: %s"
		args = append(args, f.Name())
	}
	s += ">"
	return fmt.Sprintf(s, args...)
}

func (p *Partition) setFile(f *os.File) error {
	if p.closer != nil {
		if err := p.closer.Close(); err != nil {
			return err
		}
	}

	p.reader = f
	p.closer = f
	return nil
}

func (p *Partition) setReader(r io.Reader) {
	p.reader = r
}

// Reader implements Partitioner. It can be used to access the underlying
// reader, mainly so the stdlib's sendfile can be used via
// TCPConn.ReadFrom(io.LimitRead(os.File))
func (p *Partition) Reader() io.Reader {
	return p.reader
}

func (p *Partition) Read(b []byte) (int, error) {
	return p.reader.Read(b)
}

// Close implements Partitioner
func (p *Partition) Close() error {
	return p.closer.Close()
}

// Offset implements Partitioner
func (p *Partition) Offset() uint64 {
	return p.offset
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

func partitionPath(conf *config.Config, off uint64) string {
	_, prefix := filepath.Split(conf.LogFile)
	return prefix + strconv.FormatUint(off, 10) + ".log"
}

func partitionFullPath(conf *config.Config, off uint64) string {
	dir, _ := filepath.Split(conf.LogFile)
	return filepath.Join(dir, partitionPath(conf, off))
}
