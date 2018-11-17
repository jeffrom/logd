package logger

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
)

// ErrNotFound is returned when a partition could not be found
var ErrNotFound = errors.New("partition not found")

// PartitionManager gets, create, and otherwise manages partitions
type PartitionManager interface {
	// Remove deletes a partition. If the partition doesn't exist, return an
	// error.
	Remove(off uint64) error
	// Get returns an io.Reader representing the partition located at offset,
	// its start position seeked to delta, and limited to limit bytes.
	Get(offset uint64, delta, limit int) (Partitioner, error)
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
	topic      string
	partitions []Partitioner
	tempDir    string

	refs map[uint64]int
	mu   sync.Mutex

	pathb     *bytes.Buffer
	pathCache map[string]map[uint64]string
}

// NewPartitions returns an instance of Partitions, which implements
// PartitionManager
func NewPartitions(conf *config.Config, topic string) *Partitions {
	p := &Partitions{
		conf:       conf,
		topic:      topic,
		partitions: make([]Partitioner, conf.MaxPartitions),
		refs:       make(map[uint64]int),
		pathb:      &bytes.Buffer{},
		pathCache:  make(map[string]map[uint64]string),
	}

	p.pathCache[conf.WorkDir] = make(map[uint64]string)
	return p
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
		// TODO config for tempdir location, file mode?
		tpath := filepath.Join(p.conf.WorkDir, "..", "tmp")
		if strings.HasPrefix(p.conf.WorkDir, os.TempDir()) {
			tdir, _ := filepath.Split(p.conf.WorkDir)
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
		p.pathCache[p.tempDir] = make(map[uint64]string)
	}
	return nil
}

// Remove implements PartitionManager
func (p *Partitions) Remove(off uint64) error {
	if err := p.ensureTempDir(); err != nil {
		return err
	}

	fname := partitionPath(p.conf, p.topic, off)
	tmpdir := filepath.Join(p.tempDir, p.topic)
	if err := os.MkdirAll(tmpdir, 0700); err != nil {
		return err
	}
	internal.Debugf(p.conf, "uncirculating %s", fname)
	if err := os.Rename(partitionFullPath(p.conf, p.topic, off), p.tmpPath(off)); err != nil {
		return err
	}

	if p.getRefs(off) <= 0 {
		return p.removeFile(off)
	}

	delete(p.pathCache[p.conf.WorkDir], off)
	return nil
}

func (p *Partitions) lookup(workdir string, off uint64) (string, bool) {
	d, ok := p.pathCache[workdir]
	if !ok {
		panic("uncached working directory: " + workdir)
	}

	s, ok := d[off]
	return s, ok
}

func (p *Partitions) filePath(workdir string, off uint64) string {
	if s, ok := p.lookup(workdir, off); ok {
		return s
	}

	p.pathb.Reset()
	p.pathb.WriteString(path.Clean(workdir))
	p.pathb.WriteString("/")
	p.pathb.WriteString(p.topic)
	p.pathb.WriteString("/")
	p.pathb.WriteString(strconv.FormatUint(off, 10))
	p.pathb.WriteString(".log")

	s := p.pathb.String()
	p.pathCache[workdir][off] = s
	return s
}

// Get implements PartitionManager
func (p *Partitions) Get(off uint64, delta, limit int) (Partitioner, error) {
	fname := p.filePath(p.conf.WorkDir, off)
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
	return p.list(path.Join(p.conf.WorkDir, p.topic)+"/", false)
}

func (p *Partitions) listTempDir() ([]Partitioner, error) {
	// _, suf := filepath.Split(p.conf.WorkDir)
	return p.list(path.Join(p.tempDir, p.topic)+"/", true)
}

func (p *Partitions) list(prefix string, tmp bool) ([]Partitioner, error) {
	dir, file := path.Split(prefix)
	pat := path.Join(dir, file+"[0-9]*.log")
	// fmt.Println("coolpat", pat)
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

func (p *Partitions) tmpPath(off uint64) string {
	return p.filePath(p.tempDir, off)
}

// Shutdown implements internal.LifecycleManager
func (p *Partitions) Shutdown() error {
	if p.tempDir != "" {
		// TODO log any remaining uncirculated files
		internal.Debugf(p.conf, "removing directory: %s", p.tempDir)
		return os.RemoveAll(p.tempDir)
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
	logfname := path.Join(p.conf.WorkDir, p.topic) + "/"
	if tmp {
		logfname = p.tempDir + "/"
	}
	dir, suf := filepath.Split(logfname)
	if tmp {
		_, suf = filepath.Split(p.conf.WorkDir)
	}
	s := strings.TrimPrefix(filename, dir)
	s = strings.TrimPrefix(s, suf)
	s = strings.TrimPrefix(s, p.topic+"/")
	s = strings.TrimSuffix(s, ".log")
	// fmt.Println("extractOffset", filename, dir, suf, "\n", s)
	return strconv.ParseUint(s, 10, 64)
}

// removeFile deletes the file from the filesystem. Remove must have been called
// first.
func (p *Partitions) removeFile(off uint64) error {
	if p.tempDir == "" {
		return errors.New("Partitions.Remove: temp dir not set")
	}

	// we just remove it. if it's not removed, it's not in the tempdir
	ppath := partitionPath(p.conf, p.topic, off)
	fullpath := filepath.Join(p.tempDir, ppath)
	// fmt.Println("removeFile", ppath, fullpath)

	if _, err := os.Stat(fullpath); err != nil {
		if os.IsNotExist(err) {
			internal.DiscardError(err)
			return nil
		}
		return err
	}
	go func() {
		internal.Debugf(p.conf, "removing %s", fullpath)
		err := os.Remove(fullpath)
		if err != nil {
			log.Printf("error removing %s: %+v", fullpath, err)
		}
	}()
	delete(p.pathCache[p.tempDir], off)
	return nil
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
// PartitionFile is also involved in deletions. each partition on disk should
// have an associated reference count, and when it's slated for deletion, it
// should wait until all references have been closed before deleting the file
// from disk. It should move the file into a temp directory so subsequent
// requests for deleted offsets return not found errors.
type PartitionFile struct {
	conf *config.Config
}

func partitionPath(conf *config.Config, topic string, off uint64) string {
	_, prefix := filepath.Split(conf.WorkDir)
	return path.Join(prefix, topic, strconv.FormatUint(off, 10)+".log")
}

func partitionFullPath(conf *config.Config, topic string, off uint64) string {
	dir, _ := filepath.Split(conf.WorkDir)
	return filepath.Join(dir, partitionPath(conf, topic, off))
}
