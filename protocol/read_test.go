package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestWriteRead(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
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

var invalidReads = map[string][]byte{
	// "valid": []byte("READ default 0 3"),
	"no topic":      []byte("READ  0 3"),
	"zero messages": []byte("READ default 0 0"),
}

func TestReadInvalid(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	read := NewRead(conf)

	for name, b := range invalidReads {
		t.Run(name, func(t *testing.T) {
			read.Reset()
			req := NewRequestConfig(conf)
			_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(b)))
			_, rerr := read.FromRequest(req)
			if err == nil && rerr == nil {
				t.Fatalf("%s: read should not have been valid\n%q\n", name, b)
			}
		})
	}
}
