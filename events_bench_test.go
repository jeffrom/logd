package logd

import (
	"runtime/debug"
	"testing"
)

func eventQBenchConfig() *Config {
	config := NewConfig()

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
		q.add(newCommand(cmdPing))
	}
}

func BenchmarkEventQLogOne(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.add(newCommand(cmdMsg, msg))
	}
}

func BenchmarkEventQLogFive(b *testing.B) {
	b.StopTimer()
	q := startQForBench(b)
	defer stopQ(b, q)

	msg := []byte("hey i'm a message")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q.add(newCommand(cmdMsg, msg, msg, msg, msg, msg))
	}
}
