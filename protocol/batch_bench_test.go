package protocol

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkBatchWriteV2(b *testing.B) {
	conf := protocolBenchConfig()

	fixture := testhelper.LoadFixture("batch.small")
	batch := NewBatch(conf)
	if _, err := batch.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture))); err != nil {
		b.Fatal(err)
	}

	w := ioutil.Discard

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := batch.WriteTo(w); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchWriteNewV2(b *testing.B) {
	conf := protocolBenchConfig()

	batch := NewBatch(conf)
	batch.AppendMessage(newTestMessage(conf, string(testhelper.SomeLines[0])))
	batch.AppendMessage(newTestMessage(conf, string(testhelper.SomeLines[1])))
	batch.AppendMessage(newTestMessage(conf, string(testhelper.SomeLines[2])))

	w := ioutil.Discard

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := batch.WriteTo(w); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchReadV2(b *testing.B) {
	conf := protocolBenchConfig()
	batch := NewBatch(conf)
	fixture := testhelper.LoadFixture("batch.medium")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := batch.ReadFrom(br); err != nil {
			b.Fatal(err)
		}

		batch.Reset()
		buf.Reset()
		buf.Write(fixture)
		br.Reset(buf)
	}
}
