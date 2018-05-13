package protocol

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkBatchWrite(b *testing.B) {
	conf := protocolBenchConfig()

	batch := NewBatch(conf)
	batch.AppendMessage(newTestMessage(conf, string(testhelper.SomeLines[0])))
	batch.AppendMessage(newTestMessage(conf, string(testhelper.SomeLines[1])))
	batch.AppendMessage(newTestMessage(conf, string(testhelper.SomeLines[2])))

	w := ioutil.Discard

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := batch.WriteTo(w); err != nil {
			panic(err)
		}
	}
}

func BenchmarkBatchRead(b *testing.B) {
	conf := protocolBenchConfig()
	batch := NewBatch(conf)
	fixture := testhelper.LoadFixture("batch.medium")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := batch.ReadFrom(br); err != nil {
			panic(err)
		}

		batch.Reset()
		buf.Reset()
		buf.Write(fixture)
		br.Reset(buf)
	}
}

func BenchmarkBatchResponseWrite(b *testing.B) {
	conf := protocolBenchConfig()
	batchResp := NewBatchResponse(conf)
	batchResp.SetOffset(1000)
	batchResp.SetPartition(10)
	w := ioutil.Discard

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := batchResp.WriteTo(w); err != nil {
			panic(err)
		}
	}
}
