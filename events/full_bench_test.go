package events

import (
	"bufio"
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logd"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkBatchFull(b *testing.B) {
	b.SetParallelism(4)
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.Timeout = 5 * time.Second
	conf.IdleTimeout = 5 * time.Second
	conf.Host = ":0"

	benchmarkBatchFull(b, conf, "batch.small", []string{"default"})
}

func BenchmarkBatchFullLarge(b *testing.B) {
	b.SetParallelism(4)
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.Timeout = 5 * time.Second
	conf.IdleTimeout = 5 * time.Second
	conf.Host = ":0"
	conf.MaxBatchSize = 1024 * 64
	conf.PartitionSize = 1024 * 1024

	benchmarkBatchFull(b, conf, "batch.large", []string{"default"})
}

func BenchmarkBatchFullTopics8(b *testing.B) {
	b.SetParallelism(2)
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.Timeout = 5 * time.Second
	conf.IdleTimeout = 5 * time.Second
	conf.Host = ":0"

	benchmarkBatchFull(b, conf, "batch.small", []string{
		"default", "topic1", "topic2", "topic3",
		"topic4", "topic5", "topic6", "topic7",
	})
}

func BenchmarkReadFull(b *testing.B) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.Timeout = 5 * time.Second
	conf.IdleTimeout = 5 * time.Second
	conf.MaxBatchSize = 65535
	conf.PartitionSize = conf.MaxBatchSize * 100
	conf.Host = ":0"

	benchmarkReadFull(b, conf)
}

func BenchmarkReadFull4(b *testing.B) {
	b.SetParallelism(4)
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.Timeout = 5 * time.Second
	conf.IdleTimeout = 5 * time.Second
	conf.MaxBatchSize = 65535
	conf.PartitionSize = conf.MaxBatchSize * 100
	conf.Host = ":0"

	benchmarkReadFull(b, conf)
}

type repeater struct {
	mu sync.Mutex
	n  int
	i  int
}

func newRepeater(n int) *repeater {
	return &repeater{
		n: n,
	}
}

func (r *repeater) next() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.i
	r.i++
	if r.i > r.n-1 {
		r.i = 0
	}
	return i
}

func benchmarkBatchFull(b *testing.B, conf *config.Config, fixturename string, topics []string) {
	h := NewHandlers(conf)
	if err := h.GoStart(); err != nil {
		b.Fatal(err)
	}
	defer shutdownHandlers(b, h)
	addr := h.servers[0].ListenAddr().String()
	fixture := testhelper.LoadFixture(fixturename)

	fixtures := make([][]byte, len(topics))
	for i, topic := range topics {
		f := make([]byte, len(fixture))
		copy(f, fixture)
		f = bytes.Replace(f, []byte("default"), []byte(topic), 1)
		fixtures[i] = f
	}

	r := newRepeater(len(fixtures))

	batch := protocol.NewBatch(conf)
	buf := bytes.NewBuffer(fixtures[r.next()])
	br := bufio.NewReader(buf)
	if _, err := batch.ReadFrom(br); err != nil {
		b.Fatal(err)
	}

	c, err := logd.Dial(addr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Batch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadFull(b *testing.B, conf *config.Config) {
	h := NewHandlers(conf)
	if err := h.GoStart(); err != nil {
		b.Fatal(err)
	}
	defer shutdownHandlers(b, h)
	addr := h.servers[0].ListenAddr().String()

	fixture := testhelper.LoadFixture("words.txt")
	fillTopic(b, conf, h, fixture)

	c, err := logd.Dial(addr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	topic := []byte("default")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, bs, err := c.ReadOffset(topic, 0, 3)
		if err != nil {
			b.Fatal(err)
		}

		for bs.Scan() {
		}
		if err := bs.Error(); err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}
}

func fillTopic(b *testing.B, conf *config.Config, h *Handlers, data []byte) {
	// c, err := logd.Dial(h.servers[0].ListenAddr().String())
	// if err != nil {
	// 	b.Fatal(err)
	// }

	s := bufio.NewScanner(bytes.NewBuffer(data))
	s.Split(bufio.ScanLines)
	cconf := logd.DefaultConfig.FromGeneralConfig(conf)
	cconf.Hostport = h.servers[0].ListenAddr().String()
	w := logd.NewWriter(cconf, "default")
	defer w.Close()
	// read := 0

	for s.Scan() {
		// read += len(s.Bytes()) + len(fmt.Sprintf("MSG %d\r\n\r\n", len(s.Bytes())))

		buf := make([]byte, len(s.Bytes()))
		copy(buf, s.Bytes())
		_, err := w.Write(buf)
		if err != nil {
			b.Fatal(err)
		}
	}

	w.Flush()
}

func shutdownHandlers(b *testing.B, h *Handlers) {
	if err := h.Stop(); err != nil {
		b.Fatal("shutdown failed:", err)
	}
}
