package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkRequestRead(b *testing.B) {
	conf := protocolBenchConfig()
	req := NewRequestConfig(conf)
	fixture := testhelper.LoadFixture("batch.medium")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := req.ReadFrom(br); err != nil {
			b.Fatal(err)
		}

		req.Reset()
		buf.Reset()
		buf.Write(fixture)
		br.Reset(buf)
	}
}
