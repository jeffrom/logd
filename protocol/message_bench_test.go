package protocol

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkMessageWrite(b *testing.B) {
	conf := protocolBenchConfig()
	msg := newTestMessage(conf, string(testhelper.SomeLines[0]))
	w := ioutil.Discard

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.WriteTo(w); err != nil {
			panic(err)
		}
	}
}

func BenchmarkMessageRead(b *testing.B) {
	conf := protocolBenchConfig()
	msg := NewMessageV2(conf)
	fixture := testhelper.LoadFixture("msg.medium")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.ReadFrom(br); err != nil {
			panic(err)
		}

		msg.reset()
		buf.Reset()
		buf.Write(fixture)
		br.Reset(buf)
	}
}
