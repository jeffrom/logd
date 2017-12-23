package logd

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

type stats struct {
	startedAt time.Time

	counts  map[string]int64
	countMu sync.Mutex

	histograms  map[string]histogram
	histogramMu sync.Mutex
}

var allStatKeys = []string{
	"connections",
	"subscriptions",
	"total_bytes_read",
	"total_bytes_written",
	"total_connections",
	"total_reads",
	"total_subscriptions",
	"total_writes",
}

func newStats() *stats {

	s := &stats{
		startedAt: time.Now().UTC(),
		counts:    make(map[string]int64),
	}

	for _, k := range allStatKeys {
		s.counts[k] = 0
	}

	return s
}

func (s *stats) set(key string, val int64) {
	s.countMu.Lock()
	defer s.countMu.Unlock()

	s.counts[key] = val
}

func (s *stats) add(key string, val int64) {
	s.countMu.Lock()
	defer s.countMu.Unlock()

	s.counts[key] += val
}

func (s *stats) incr(key string) {
	s.add(key, 1)
}

func (s *stats) decr(key string) {
	s.add(key, -1)
}

func (s *stats) bytes() []byte {
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

		buf.WriteString(k)
		buf.WriteString(": ")

		buf.WriteString(strconv.FormatInt(v, 10))
		buf.WriteString("\r\n")
	}

	// fmt.Printf("%q\n", buf.Bytes())
	return buf.Bytes()
}
