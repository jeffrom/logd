package logd

import (
	"io"
	"io/ioutil"
	"log"
	"runtime/debug"
	"testing"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func eventQBenchConfig() *Config {
	config := NewConfig()
	config.ServerTimeout = 500
	config.ClientTimeout = 500
	config.MaxChunkSize = 1024 * 10
	config.IndexCursorSize = 1000
	config.PartitionSize = 2048

	config.Verbose = false

	return config
}

func startQForBench(b *testing.B) (*eventQ, func()) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.LogFile = tmpLog()
	config, _, shutdown := setupFileLoggerConfig(b, config)

	q := newEventQ(config)
	if err := q.start(); err != nil {
		b.Logf("%s", debug.Stack())
		b.Fatalf("error starting queue: %v", err)
	}
	return q, shutdown
}

func BenchmarkEventQLifecycle(b *testing.B) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.LogFile = tmpLog()

	for i := 0; i < b.N; i++ {
		_, _, shutdown := setupFileLoggerConfig(b, config)
		q := newEventQ(config)
		q.start()
		q.handleShutdown(nil)
		q.stop()
		shutdown()
	}
}

func BenchmarkEventQPing(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdPing))
	}
}

func BenchmarkEventQLogOne(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdMessage, msg))
	}
}

func BenchmarkEventQLogFive(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdMessage, msg, msg, msg, msg, msg))
	}
}

func BenchmarkEventQReadOne(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	q.pushCommand(NewCommand(config, CmdMessage, []byte("hey i'm a message")))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cmd := NewCommand(config, CmdRead, []byte("1"), []byte("1"))
		resp, _ := q.pushCommand(cmd)
		cmd.signalReady()
		drainReaderChan(resp.readerC)
	}
}

func BenchmarkEventQReadFromHeadOne(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	cmd := NewCommand(config, CmdRead, []byte("1"), []byte("0"))
	resp, _ := q.pushCommand(cmd)
	cmd.signalReady()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdMessage, msg))
		drainReaderChan(resp.readerC)
	}
}

func BenchmarkEventQReadFromHeadTen(b *testing.B) {
	b.SkipNow()
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)
	msg := []byte("hey i'm a message")

	var subs []*Response
	for i := 0; i < 10; i++ {
		cmd := NewCommand(config, CmdRead, []byte("1"), []byte("0"))
		resp, _ := q.pushCommand(cmd)
		cmd.signalReady()
		subs = append(subs, resp)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdMessage, msg))
		for _, resp := range subs {
			drainReaderChan(resp.readerC)
		}
	}
}

func drainReaderChan(readerC chan io.Reader) {
	n := 0
	for {
		select {
		case <-readerC:
			n++
			// case <-time.After(time.Millisecond * 200):
			// 	panic("timed out waiting for messages on readerC")
		default:
			if n > 0 {
				return
			}
		}
	}
}
