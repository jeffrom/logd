package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func newTestMessage(conf *config.Config, body string) *Message {
	msg := NewMessage(conf)
	msg.SetBody([]byte(body))
	return msg
}

func TestWriteMessage(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	s := "cool message"
	msg := newTestMessage(conf, s)

	b := &bytes.Buffer{}
	msg.WriteTo(b)
	testhelper.CheckGoldenFile("msg.small", b.Bytes(), testhelper.Golden)

	b.Reset()
	msg.Reset()
	msg.SetBody([]byte(s))
	msg.WriteTo(b)
	testhelper.CheckGoldenFile("msg.small", b.Bytes(), testhelper.Golden)
}

func TestReadMessage(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	msg := NewMessage(conf)
	fixture := testhelper.LoadFixture("msg.small")
	buf := bytes.NewBuffer(fixture)
	br := bufio.NewReaderSize(buf, buf.Len())
	s := "cool message"

	if _, err := msg.ReadFrom(br); err != nil {
		t.Fatalf("(ReadFrom) unexpected error: %+v", err)
	}
	if !bytes.Equal(msg.BodyBytes(), []byte(s)) {
		t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", s, msg.BodyBytes())
	}
	if msg.Size != 12 {
		t.Fatalf("expected size to be 12 but was %d", msg.Size)
	}

	buf.Reset()
	buf.Write(fixture)
	br.Reset(buf)
	msg.Reset()

	if _, err := msg.ReadFrom(br); err != nil {
		t.Fatalf("(ReadFrom) unexpected error: %+v", err)
	}
	if !bytes.Equal(msg.BodyBytes(), []byte(s)) {
		t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q\n", s, msg.BodyBytes())
	}
	if msg.Size != 12 {
		t.Fatalf("expected size to be 12 but was %d", msg.Size)
	}
}
