package events

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkBatchFull(b *testing.B) {
	b.SetParallelism(4)
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	conf.Hostport = ":0"
	q := NewEventQ(conf)
	if err := q.GoStart(); err != nil {
		b.Fatal(err)
	}
	addr := q.servers[0].ListenAddr().String()

	b.ResetTimer()
	b.RunParallel(func(b *testing.PB) {
		fixture := testhelper.LoadFixture("batch.small")
		batch := protocol.NewBatch(conf)
		buf := bytes.NewBuffer(fixture)
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
