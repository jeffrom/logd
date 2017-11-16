package logd

import (
	"io/ioutil"
	"log"
	"testing"
)

func fileLoggerBenchConfig() *Config {
	config := testConfig(nil)
	config.MaxChunkSize = 1024 * 10
	config.PartitionSize = 1024 * 1024 * 500
	config.IndexCursorSize = 100
	config.LogFile = tmpLog()

	log.SetOutput(ioutil.Discard)
	return config
}

func BenchmarkFileLoggerLifecycle(b *testing.B) {
	config := fileLoggerBenchConfig()
	l := newFileLogger(config)

	for i := 0; i < b.N; i++ {
		l.Setup()
		l.Shutdown()
	}
}

func BenchmarkFileLoggerWrite(b *testing.B) {
	config := fileLoggerBenchConfig()
	l := newFileLogger(config)
	l.Setup()
	defer l.Shutdown()

	for i := 0; i < b.N; i++ {
		l.Write(someMessage)
	}
}

func BenchmarkFileLoggerWriteWithFlush(b *testing.B) {
	config := fileLoggerBenchConfig()
	l := newFileLogger(config)
	l.Setup()
	defer l.Shutdown()

	for i := 0; i < b.N; i++ {
		l.Write(someMessage)
		l.Flush()
	}
}

// TODO act on prepopulated logs / partitions
