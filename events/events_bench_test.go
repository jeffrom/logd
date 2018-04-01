package events

import (
	"context"
	"fmt"
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
	config.GracefulShutdownTimeout = 500
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
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdPing))
	}
}

func BenchmarkEventQLogOne(b *testing.B) {
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, msg))
		if err != nil {
			panic(err)
		}

		if resp.Status != protocol.RespOK {
			log.Panicf("expected ok response but got %s", resp)
		}
	}
}

func BenchmarkEventQLogFive(b *testing.B) {
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, msg, msg, msg, msg, msg))
		if err != nil {
			panic(err)
		}

		if resp.Status != protocol.RespOK {
			log.Panicf("expected ok response but got %s", resp)
		}
	}
}

func BenchmarkEventQReadOne(b *testing.B) {
	b.SkipNow()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, []byte("hey i'm a message")))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Println(i)
		cmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("1"))
		resp, _ := q.PushCommand(context.Background(), cmd)
		cmd.SignalReady()
		drainReaderChan(resp.ReaderC)
	}
}

func BenchmarkEventQReadFromTailOne(b *testing.B) {
	b.SkipNow()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)

	q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, []byte("hey i'm a message")))

	msg := []byte("hey i'm a message")
	cmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("0"))
	resp, err := q.PushCommand(context.Background(), cmd)
	testhelper.CheckError(err)
	if resp.Status != protocol.RespOK {
		panic("unexpected failure response")
	}
	cmd.SignalReady()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := protocol.NewCommand(config, protocol.CmdMessage, msg)
		ctx, cancel := context.WithCancel(context.Background())
		resp, err := q.PushCommand(ctx, cmd)
		testhelper.CheckError(err)
		if resp.Status != protocol.RespOK {
			panic("unexpected failure response")
		}
		drainReaderChan(resp.ReaderC)
		cancel()
	}
}

func BenchmarkEventQReadFromHeadTen(b *testing.B) {
	b.SkipNow()
	config := eventQBenchConfig()
	q, shutdown := startQForBench(b)
	defer shutdown()
	defer stopQ(b, q)
	msg := []byte("hey i'm a message")

	q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, []byte("hey i'm a message")))

	var subs []*protocol.Response
	for i := 0; i < 10; i++ {
		cmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("0"))
		resp, _ := q.PushCommand(context.Background(), cmd)
		cmd.SignalReady()
		subs = append(subs, resp)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, msg))
		for _, resp := range subs {
			drainReaderChan(resp.ReaderC)
		}
	}
}

func drainReaderChan(readerC chan protocol.ReadPart) {
	n := 0
	for {
		select {
		case <-readerC:
			n++
		case <-time.After(time.Millisecond * 500):
			panic("timed out waiting for messages on readerC")
		default:
			if n > 0 {
				return
			}
		}
	}
}
