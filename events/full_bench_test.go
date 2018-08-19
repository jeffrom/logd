package events

import (
	"bufio"
	"bytes"
	"sync"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkBatchFull(b *testing.B) {
	b.SetParallelism(4)
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	conf.Hostport = ":0"
	benchmarkBatchFull(b, conf, "batch.small", []string{"default"})
}

func BenchmarkBatchFullLarge(b *testing.B) {
	b.SetParallelism(4)
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	conf.Hostport = ":0"
	conf.MaxBatchSize = 1024 * 64
	conf.PartitionSize = 1024 * 1024
	benchmarkBatchFull(b, conf, "batch.large", []string{"default"})
}

// func BenchmarkBatchFullNewTopic(b *testing.B) {
// 	b.SetParallelism(4)
// 	conf := testhelper.DefaultTestConfig(testing.Verbose())
// 	conf.Hostport = ":0"
// 	benchmarkBatchFull(b, conf, "batch.small", []string{"topic1"})
// }

// func BenchmarkBatchFullTopics2(b *testing.B) {
// 	b.SetParallelism(2)
// 	conf := testhelper.DefaultTestConfig(testing.Verbose())
// 	conf.Hostport = ":0"
// 	benchmarkBatchFull(b, conf, "batch.small", []string{"default", "topic1"})
// }

// func BenchmarkBatchFullTopics4(b *testing.B) {
// 	b.SetParallelism(2)
// 	conf := testhelper.DefaultTestConfig(testing.Verbose())
// 	conf.Hostport = ":0"
// 	benchmarkBatchFull(b, conf, "batch.small", []string{"default", "topic1", "topic2", "topic3"})
// }

func BenchmarkBatchFullTopics8(b *testing.B) {
	b.SetParallelism(2)
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	conf.Hostport = ":0"
	benchmarkBatchFull(b, conf, "batch.small", []string{
		"default", "topic1", "topic2", "topic3",
		"topic4", "topic5", "topic6", "topic7",
	})
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

	b.ResetTimer()
	b.RunParallel(func(b *testing.PB) {
		batch := protocol.NewBatch(conf)
		buf := bytes.NewBuffer(fixtures[r.next()])
		br := bufio.NewReader(buf)
		if _, err := batch.ReadFrom(br); err != nil {
			panic(err)
		}

		c, err := client.Dial(addr)
		if err != nil {
			panic(err)
		}
		defer c.Close()

		for b.Next() {
			if _, err := c.Batch(batch); err != nil {
				panic(err)
			}
		}
	})
}
