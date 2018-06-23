package protocol

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkMessageWriteV2(b *testing.B) {
	conf := protocolBenchConfig()
	msg := newTestMessage(conf, string(testhelper.SomeLines[0]))
	w := ioutil.Discard

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.WriteTo(w); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessageReadV2(b *testing.B) {
	conf := protocolBenchConfig()
	msg := NewMessageV2(conf)
	fixture := testhelper.LoadFixture("msg.small")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.ReadFrom(br); err != nil {
			b.Fatal(err)
		}

		msg.Reset()
		buf.Reset()
		buf.Write(fixture)
		br.Reset(buf)
	}
}
