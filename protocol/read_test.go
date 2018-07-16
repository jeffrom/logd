package protocol

import (
	"bytes"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestWriteRead(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	read := NewRead(conf)
	read.Offset = 1234567
	read.Messages = 100
	read.SetTopic([]byte("default"))

	b := &bytes.Buffer{}
	if _, err := read.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing READ request: %v", err)
	}

	testhelper.CheckGoldenFile("read.simple", b.Bytes(), testhelper.Golden)
}
