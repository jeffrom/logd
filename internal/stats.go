package internal

import (
	"bytes"
	"sort"
	"strconv"
	"sync"
	"time"
)

type histogramBucket struct {
}

type histogram struct {
	buckets []histogramBucket
}

// Stats is a struct containing internal counters
type Stats struct {
	startedAt time.Time

	counts  map[string]int64
	countMu sync.Mutex

	histograms  map[string]histogram
	histogramMu sync.Mutex
}

var allStatKeys = []string{
	"command_errors",
	"connections",
	"connection_errors",
	"subscriptions",
	"total_bytes_read",
	"total_bytes_written",
	"total_connections",
	"total_reads",
	"total_subscriptions",
	"total_writes",
}

// NewStats returns a new instance of Stats
func NewStats() *Stats {

	s := &Stats{
		startedAt: time.Now().UTC(),
		counts:    make(map[string]int64),
	}

	for _, k := range allStatKeys {
		s.counts[k] = 0
	}

	return s
}

func (s *Stats) Set(key string, val int64) {
	s.countMu.Lock()
	defer s.countMu.Unlock()

	s.counts[key] = val
}

func (s *Stats) Add(key string, val int64) {
	s.countMu.Lock()
	defer s.countMu.Unlock()

	s.counts[key] += val
}

func (s *Stats) Incr(key string) {
	s.Add(key, 1)
}

func (s *Stats) Decr(key string) {
	s.Add(key, -1)
}

func (s *Stats) Bytes() []byte {
	s.countMu.Lock()
	defer s.countMu.Unlock()

	buf := bytes.NewBuffer([]byte{})
	var keys []string

	for k := range s.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := s.counts[k]

		writeStringOrPanic(buf, k)
		writeStringOrPanic(buf, ": ")

		writeStringOrPanic(buf, strconv.FormatInt(v, 10))
		writeStringOrPanic(buf, "\r\n")
	}

	// fmt.Printf("%q\n", buf.Bytes())
	return buf.Bytes()
}

func writeStringOrPanic(buf *bytes.Buffer, s string) {
	if _, err := buf.WriteString(s); err != nil {
		panic(err)
	}
}
