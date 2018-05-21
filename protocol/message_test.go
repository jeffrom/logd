package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func newTestMessage(conf *config.Config, body string) *MessageV2 {
	msg := NewMessageV2(conf)
	msg.SetBody([]byte(body))
	return msg
}

func TestWriteMessage(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	msg := newTestMessage(conf, "cool message")

	b := &bytes.Buffer{}
	msg.WriteTo(b)
	testhelper.CheckGoldenFile("msg.small", b.Bytes(), testhelper.Golden)
}

func TestReadMessage(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	msg := NewMessageV2(conf)
	fixture := testhelper.LoadFixture("msg.small")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())

	if _, err := msg.ReadFrom(br); err != nil {
		t.Fatalf("(ReadFrom) unexpected error: %+v", err)
	}
}
