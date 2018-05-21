package logger

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

var someMessage = []byte("cool, reasonably-sized message. something about the length of an access log, or a json object displaying some information about a request. Not too big, not too small. Probably about 200 bytes, maybe more. I mean, these things are pretty arbitrary, really. In many instances, 200 bytes would be far too small. In others, too large.")

func fileLoggerBenchConfig() *config.Config {
	config := testhelper.TestConfig(testing.Verbose())
	config.MaxBatchSize = 1024 * 10
	config.PartitionSize = 1024 * 1024 * 500
	config.IndexCursorSize = 100
	config.LogFile = testhelper.TmpLog()

	log.SetOutput(ioutil.Discard)
	return config
}

func BenchmarkFileLoggerLifecycle(b *testing.B) {
	config := fileLoggerBenchConfig()
	l := New(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Setup()
		l.Shutdown()
	}
}

func BenchmarkFileLoggerWrite(b *testing.B) {
	config := fileLoggerBenchConfig()
	l := New(config)
	l.Setup()
	defer l.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Write(someMessage)
	}
}

func BenchmarkFileLoggerWriteWithFlush(b *testing.B) {
	config := fileLoggerBenchConfig()
	l := New(config)
	l.Setup()
	defer l.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Write(someMessage)
		l.Flush()
	}
}

// TODO act on prepopulated logs / partitions
