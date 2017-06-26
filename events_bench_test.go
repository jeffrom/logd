package logd

import (
	"runtime/debug"
	"testing"
)

func eventQBenchConfig() *ServerConfig {
	config := NewServerConfig()

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

func BenchmarkEventQPing(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(CmdPing))
	}
}

func BenchmarkEventQLogOne(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(CmdMessage, msg))
	}
}

func BenchmarkEventQLogFive(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(CmdMessage, msg, msg, msg, msg, msg))
	}
}

func BenchmarkEventQReadOne(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)
	q.pushCommand(NewCommand(CmdMessage, []byte("hey i'm a message")))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := q.pushCommand(NewCommand(CmdRead, []byte("1"), []byte("1")))
		<-resp.msgC
	}
}

func BenchmarkEventQReadFromHeadOne(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)
	msg := []byte("hey i'm a message")
	resp, _ := q.pushCommand(NewCommand(CmdRead, []byte("1"), []byte("0")))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(CmdMessage, msg))
		<-resp.msgC
	}
}

func BenchmarkEventQReadFromHeadTen(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)
	msg := []byte("hey i'm a message")

	var subs []*Response
	for i := 0; i < 10; i++ {
		resp, _ := q.pushCommand(NewCommand(CmdRead, []byte("1"), []byte("0")))
		subs = append(subs, resp)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.pushCommand(NewCommand(CmdMessage, msg))
		for _, resp := range subs {
			<-resp.msgC
		}
	}
}
