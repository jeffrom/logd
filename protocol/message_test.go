package protocol

import (
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
