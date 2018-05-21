package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkRequestReadV2(b *testing.B) {
	conf := protocolBenchConfig()
	req := NewRequest(conf)
	fixture := testhelper.LoadFixture("batch.medium")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := req.ReadFrom(br); err != nil {
			b.Fatal(err)
		}

		req.reset()
		buf.Reset()
		buf.Write(fixture)
		br.Reset(buf)
	}
}
