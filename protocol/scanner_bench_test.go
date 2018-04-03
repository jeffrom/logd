package protocol

import (
	"bytes"
	"io"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func protocolBenchConfig() *config.Config {
	return config.DefaultConfig
}

func BenchmarkScannerReadMessages(b *testing.B) {
	conf := protocolBenchConfig()
	buf := testhelper.LoadFixture("proper")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewScanner(conf, bytes.NewBuffer(buf))
		for s.Scan() {
		}
		if err := s.Error(); err != nil && err != io.EOF {
			panic(err)
		}
	}
}
