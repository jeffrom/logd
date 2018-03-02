package events

import (
	"io"
	"io/ioutil"
	"log"
	"runtime/debug"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func eventQBenchConfig() *config.Config {
	config := config.NewConfig()
	config.ServerTimeout = 500
	config.ClientTimeout = 500
	config.MaxChunkSize = 1024 * 10
	config.IndexCursorSize = 1000
	config.PartitionSize = 2048

	config.Verbose = false

	return config
}

func startQForBench(b *testing.B) (*EventQ, func()) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.LogFile = testhelper.TmpLog()
	config, _, shutdown := logger.SetupTestFileLoggerConfig(config, testing.Verbose())

	q := NewEventQ(config)
	if err := q.Start(); err != nil {
		b.Logf("%s", debug.Stack())
		b.Fatalf("error starting queue: %v", err)
	}
	return q, shutdown
}

func BenchmarkEventQLifecycle(b *testing.B) {
	config := eventQBenchConfig()
	config.LogFileMode = 0644
	config.LogFile = testhelper.TmpLog()

	for i := 0; i < b.N; i++ {
		_, _, shutdown := logger.SetupTestFileLoggerConfig(config, testing.Verbose())
		q := NewEventQ(config)
		q.Start()
		q.HandleShutdown(nil)
		q.Stop()
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
		q.PushCommand(protocol.NewCommand(config, protocol.CmdPing))
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
		q.PushCommand(protocol.NewCommand(config, protocol.CmdMessage, msg))
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
		q.PushCommand(protocol.NewCommand(config, protocol.CmdMessage, msg, msg, msg, msg, msg))
	}
}

func BenchmarkEventQReadOne(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	q.PushCommand(protocol.NewCommand(config, protocol.CmdMessage, []byte("hey i'm a message")))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("1"))
		resp, _ := q.PushCommand(cmd)
		cmd.SignalReady()
		drainReaderChan(resp.ReaderC)
	}
}

func BenchmarkEventQReadFromHeadOne(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	q.PushCommand(protocol.NewCommand(config, protocol.CmdMessage, []byte("hey i'm a message")))

	msg := []byte("hey i'm a message")
	cmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("0"))
	resp, _ := q.PushCommand(cmd)
	cmd.SignalReady()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.PushCommand(protocol.NewCommand(config, protocol.CmdMessage, msg))
		drainReaderChan(resp.ReaderC)
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

	q.PushCommand(protocol.NewCommand(config, protocol.CmdMessage, []byte("hey i'm a message")))

	var subs []*protocol.Response
	for i := 0; i < 10; i++ {
		cmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("0"))
		resp, _ := q.PushCommand(cmd)
		cmd.SignalReady()
		subs = append(subs, resp)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.PushCommand(protocol.NewCommand(config, protocol.CmdMessage, msg))
		for _, resp := range subs {
			drainReaderChan(resp.ReaderC)
		}
	}
}

func drainReaderChan(readerC chan io.Reader) {
	n := 0
	for {
		select {
		case <-readerC:
			n++
		case <-time.After(time.Millisecond * 200):
			panic("timed out waiting for messages on readerC")
		default:
			if n > 0 {
				return
			}
		}
	}
}
