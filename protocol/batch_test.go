package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestWriteBatch(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	batch := NewBatch(conf)
	batch.AppendMessage(newTestMessage(conf, "hi"))
	batch.AppendMessage(newTestMessage(conf, "hallo"))
	batch.AppendMessage(newTestMessage(conf, "sup"))

	b := &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}
	testhelper.CheckGoldenFile("batch.small", b.Bytes(), testhelper.Golden)

	batch.Reset()
	batch.Append([]byte("hi"))
	batch.Append([]byte("hallo"))
	batch.Append([]byte("sup"))

	b.Reset()

	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}
	testhelper.CheckGoldenFile("batch.small", b.Bytes(), testhelper.Golden)
}

func TestWriteBatchSmallest(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	batch := NewBatch(conf)
	batch.AppendMessage(newTestMessage(conf, "0"))

	b := &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}

	testhelper.CheckGoldenFile("batch.smallest", b.Bytes(), testhelper.Golden)
}

func TestReadBatch(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	batch := NewBatch(conf)
	fixture := testhelper.LoadFixture("batch.small")
	b := &bytes.Buffer{}
	b.Write(fixture)
	br := bufio.NewReader(b)
	if _, err := batch.ReadFrom(br); err != nil {
		t.Fatalf("unexpected error reading batch: %v", err)
	}

	b = &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}

	actual := b.Bytes()
	testhelper.CheckGoldenFile("batch.small", actual, testhelper.Golden)
}
