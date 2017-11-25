package logd

import (
	"runtime/debug"
	"testing"
)

func eventQBenchConfig() *Config {
	config := NewConfig()
	config.ServerTimeout = 500
	config.ClientTimeout = 500
	config.MaxChunkSize = 1024 * 10
	config.IndexCursorSize = 1000

	logger := newMemLogger()
	// logger.returnErr = loggerShouldErr

	config.Verbose = false
	config.Logger = logger

	return config
}

func startQForBench(b *testing.B) *eventQ {
	q := newEventQ(eventQBenchConfig())
	if err := q.start(); err != nil {
		b.Logf("%s", debug.Stack())
		b.Fatalf("error starting queue: %v", err)
	}
	return q
}

func BenchmarkEventQLifecycle(b *testing.B) {
	config := eventQBenchConfig()

	for i := 0; i < b.N; i++ {
		q := newEventQ(config)
		q.start()
		q.handleShutdown(nil)
		q.stop()
	}
}

func BenchmarkEventQPing(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q := startQForBench(b)
	defer stopQ(b, q)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdPing))
	}
}

func BenchmarkEventQLogOne(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q := startQForBench(b)
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
	q := startQForBench(b)
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
	q := startQForBench(b)
	defer stopQ(b, q)
	q.pushCommand(NewCommand(config, CmdMessage, []byte("hey i'm a message")))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := q.pushCommand(NewCommand(config, CmdRead, []byte("1"), []byte("1")))
		<-resp.readerC
	}
}

func BenchmarkEventQReadFromHeadOne(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q := startQForBench(b)
	defer stopQ(b, q)
	msg := []byte("hey i'm a message")
	resp, _ := q.pushCommand(NewCommand(config, CmdRead, []byte("1"), []byte("0")))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdMessage, msg))
		<-resp.readerC
	}
}

func BenchmarkEventQReadFromHeadTen(b *testing.B) {
	b.StopTimer()
	config := eventQBenchConfig()
	q := startQForBench(b)
	defer stopQ(b, q)
	msg := []byte("hey i'm a message")

	var subs []*Response
	for i := 0; i < 10; i++ {
		resp, _ := q.pushCommand(NewCommand(config, CmdRead, []byte("1"), []byte("0")))
		subs = append(subs, resp)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(config, CmdMessage, msg))
		for _, resp := range subs {
			<-resp.readerC
		}
	}
}
