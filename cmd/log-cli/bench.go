package main

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/stats"
	"github.com/spf13/cobra"
)

var tmpBenchConfig = &benchConfig{
	conf: tmpConfig,
}

func init() {
	pflags := BenchCmd.PersistentFlags()

	pflags.IntVar(&tmpBenchConfig.batchSize, "size", 65500,
		"number of bytes to send per batch")
	pflags.IntVar(&tmpBenchConfig.conns, "conns", 1,
		"number of connections")

	pflags.DurationVar(&tmpBenchConfig.duration, "duration", 5*time.Second,
		"amount of time for the benchmark")

	pflags.IntVar(&tmpBenchConfig.topics, "topics", 1,
		"number of topics to write to")

	pflags.BoolVar(&tmpBenchConfig.onlyRead, "only-read", false,
		"read benchmark only")
	pflags.BoolVar(&tmpBenchConfig.onlyWrite, "only-write", false,
		"write benchmark only")
}

type benchConfig struct {
	conf      *client.Config
	conns     int
	batchSize int
	topics    int
	duration  time.Duration
	onlyRead  bool
	onlyWrite bool
}

const fillAmt = 10

type benchCounts struct {
	started      time.Time
	inbytes      int64
	outbytes     int64
	batches      int64
	batchesRead  int64
	messagesRead int64
	timing       *stats.Histogram
}

func newBenchCounts() *benchCounts {
	return &benchCounts{
		timing: stats.NewHistogram(),
	}
}

func (c *benchCounts) String() string {
	b := bytes.Buffer{}
	dur := time.Since(c.started).Seconds()

	if c.inbytes > 0 {
		formatted := prettyNumBytes(float64(c.inbytes))
		formattedPer := prettyNumBytes(float64(c.inbytes) / dur)
		s := fmt.Sprintf("%s:\t\t%s\t\t%s/s\n",
			fill("bytes in", fillAmt), fill(formatted, fillAmt), formattedPer)
		b.WriteString(s)
	}

	if c.outbytes > 0 {
		formatted := prettyNumBytes(float64(c.outbytes))
		formattedPer := prettyNumBytes(float64(c.outbytes) / dur)
		s := fmt.Sprintf("%s:\t\t%s\t\t%s/s\n",
			fill("bytes out", fillAmt), fill(formatted, fillAmt), formattedPer)
		b.WriteString(s)
	}

	if c.batches > 0 {
		formatted := prettyNum(float64(c.batches))
		formattedPer := prettyNum(float64(c.batches) / dur)
		s := fmt.Sprintf("%s:\t\t%s\t\t%s/s\n",
			fill("batches", fillAmt), fill(formatted, fillAmt), formattedPer)
		b.WriteString(s)
	}

	if c.batchesRead > 0 {
		formatted := prettyNum(float64(c.batchesRead))
		formattedPer := prettyNum(float64(c.batchesRead) / dur)
		s := fmt.Sprintf("%s:\t\t%s\t\t%s/s\n",
			fill("batches", fillAmt), fill(formatted, fillAmt), formattedPer)
		b.WriteString(s)
	}

	b.WriteString(fmt.Sprintf("%s:\n", fill("timing", fillAmt)))
	b.WriteString(fmt.Sprintf("\tmin %s\n", stats.PrettyTime(c.timing.Quantile(0.0))))
	b.WriteString(fmt.Sprintf("\tp50 %s\n", stats.PrettyTime(c.timing.Quantile(0.50))))
	b.WriteString(fmt.Sprintf("\tp90 %s\n", stats.PrettyTime(c.timing.Quantile(0.90))))
	b.WriteString(fmt.Sprintf("\tp95 %s\n", stats.PrettyTime(c.timing.Quantile(0.95))))
	b.WriteString(fmt.Sprintf("\tp99 %s\n", stats.PrettyTime(c.timing.Quantile(0.99))))
	b.WriteString(fmt.Sprintf("\tmax %s\n", stats.PrettyTime(c.timing.Quantile(1.0))))

	return b.String()
}

type benchConn struct {
	input  []byte
	topic  []byte
	c      *client.Client
	counts *benchCounts
	done   chan struct{}
}

func newBenchConn(c *client.Client, counts *benchCounts, input []byte, topic []byte) *benchConn {
	return &benchConn{
		c:      c,
		counts: counts,
		input:  input,
		topic:  topic,
		done:   make(chan struct{}, 1),
	}
}

func (bc *benchConn) startWrite() {
	for {
		select {
		case <-bc.done:
			bc.c.Close()
			return
		default:
		}

		start := time.Now()
		if _, err := bc.c.BatchRaw(bc.input); err != nil {
			panic(err)
		}

		if !atomic.CompareAndSwapInt64(&bc.counts.batches, 0, 1) {
			bc.counts.timing.Add(float64(time.Since(start).Nanoseconds()))
			atomic.AddInt64(&bc.counts.outbytes, int64(len(bc.input)))
			atomic.AddInt64(&bc.counts.batches, 1)
		}
	}
}

func (bc *benchConn) startRead() {
	var offset uint64

	for {
		select {
		case <-bc.done:
			bc.c.Close()
			return
		default:
		}

		var nbatches int
		var bs *protocol.BatchScanner
		var err error
		start := time.Now()
		if offset == 0 {
			offset, nbatches, bs, err = bc.c.Tail(bc.topic, 15)
		} else {
			nbatches, bs, err = bc.c.ReadOffset(bc.topic, offset, 15)
			if err == protocol.ErrNotFound {
				offset = 0
				continue
			}
		}
		if err != nil {
			panic(err)
		}

		n := 0
		for bs.Scan() {
			fullsize, _ := bs.Batch().FullSize()
			offset += uint64(fullsize)
			n++
			if n >= nbatches {
				break
			}
		}
		if err := bs.Error(); err != nil {
			panic(err)
		}

		if !atomic.CompareAndSwapInt64(&bc.counts.batchesRead, 0, int64(nbatches)) {
			bc.counts.timing.Add(float64(time.Since(start).Nanoseconds()))
			atomic.AddInt64(&bc.counts.batchesRead, int64(nbatches))
			atomic.AddInt64(&bc.counts.inbytes, int64(bs.Scanned()))
		}
	}
}

func (bc *benchConn) stop() {
	close(bc.done)
}

var BenchCmd = &cobra.Command{
	Use:   "bench",
	Short: "benchmarking tool",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("batch size: %db, topics: %d, duration: %s, connections: %d\n",
			tmpBenchConfig.batchSize, tmpBenchConfig.topics, tmpBenchConfig.duration, tmpBenchConfig.conns)
		fmt.Println("config:", tmpConfig)

		if err := doBench(tmpBenchConfig, cmd); err != nil {
			panic(err)
		}
	},
}

func doBench(bconf *benchConfig, cmd *cobra.Command) error {
	if !bconf.onlyRead {
		counts, err := benchWriteLoop(bconf)
		if err != nil {
			return err
		}
		fmt.Printf("\nwrite batch:\n\n%s\n", counts)
	}

	if !bconf.onlyWrite {
		counts, err := benchReadLoop(bconf)
		if err != nil {
			return err
		}
		fmt.Printf("\nread batch:\n\n%s\n", counts)
	}
	return nil
}

func benchWriteLoop(bconf *benchConfig) (*benchCounts, error) {
	done := time.After(bconf.duration)
	counts := newBenchCounts()
	inputs := generateBatches(bconf)

	counts.started = time.Now()
	wg := sync.WaitGroup{}
	conns, err := generateConns(bconf, counts, inputs)
	if err != nil {
		return counts, err
	}

	for i, conn := range conns {
		wg.Add(1)
		go func(idx int, conn *benchConn) {
			conn.startWrite()
			wg.Done()
		}(i, conn)
	}

	<-done

	for _, conn := range conns {
		conn.stop()
	}

	wg.Wait()

	return counts, nil
}

func benchReadLoop(bconf *benchConfig) (*benchCounts, error) {
	done := time.After(bconf.duration)
	counts := newBenchCounts()
	inputs := generateBatches(bconf)

	counts.started = time.Now()
	wg := sync.WaitGroup{}
	conns, err := generateConns(bconf, counts, inputs)
	if err != nil {
		return counts, err
	}

	for i, conn := range conns {
		wg.Add(1)
		go func(idx int, conn *benchConn) {
			conn.startRead()
			wg.Done()
		}(i, conn)
	}

	<-done

	for _, conn := range conns {
		conn.stop()
	}

	wg.Wait()

	return counts, nil
}

func generateConns(bconf *benchConfig, counts *benchCounts, inputs [][]byte) ([]*benchConn, error) {
	if len(inputs) > bconf.conns {
		fmt.Printf("warning: %d topics will not be used by %d connections (need at least one topic per connection)\n", len(inputs), bconf.conns)
	}
	var conns []*benchConn
	for i := 0; i < bconf.conns; i++ {
		c, err := client.DialConfig(bconf.conf.Hostport, bconf.conf)
		if err != nil {
			return conns, err
		}

		t := []byte(fmt.Sprintf("benchmark_%d", i%bconf.topics))
		conns = append(conns, newBenchConn(c, counts, inputs[i%len(inputs)], t))
	}
	return conns, nil
}

func generateBatches(bconf *benchConfig) [][]byte {
	var inputs [][]byte
	for i := 0; i < bconf.topics; i++ {
		inputs = append(inputs, generateBatch(bconf, fmt.Sprintf("benchmark_%d", i)))
	}
	return inputs
}

func generateBatch(bconf *benchConfig, topic string) []byte {
	b := &bytes.Buffer{}
	batch := protocol.NewBatch(bconf.conf.ToGeneralConfig())

	batch.SetTopic([]byte(topic))

	for batch.CalcSize() < bconf.batchSize {
		if err := batch.Append([]byte("oh hai sup not much idk howre u")); err != nil {
			panic(err)
		}
	}

	if _, err := batch.WriteTo(b); err != nil {
		panic(err)
	}

	return b.Bytes()
}
