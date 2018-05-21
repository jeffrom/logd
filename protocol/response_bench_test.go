package protocol

import (
	"io/ioutil"
	"testing"
)

func BenchmarkBatchResponseWriteV2(b *testing.B) {
	conf := protocolBenchConfig()
	cr := NewClientResponse(conf)
	cr.SetOffset(1000)
	w := ioutil.Discard

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cr.WriteTo(w); err != nil {
			b.Fatal(err)
		}
	}
}
