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
}

func TestReadBatch(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	batch := NewBatch(conf)
	fixture := testhelper.LoadFixture("batch.small")
	b := &bytes.Buffer{}
	b.Write(fixture)
	br := bufio.NewReader(b)
	if _, err := batch.readFromBuf(br); err != nil {
		t.Fatalf("unexpected error reading batch: %v", err)
	}

	b = &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}

	actual := b.Bytes()
	if !bytes.Equal(actual, fixture) {
		t.Fatalf("resulting batch doesn't match fixture:\n\nexpected:\n\n\t%q\n\n\nactual:\n\n\t%q", fixture, actual)
	}
}

func TestWriteBatchResponse(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	resp := NewBatchResponse(conf)
	resp.SetOffset(10)
	resp.SetPartition(1)
	fixture := testhelper.LoadFixture("batch_response.simple")
	b := &bytes.Buffer{}

	if _, err := resp.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing response: %+v", err)
	}

	actual := b.Bytes()
	if !bytes.Equal(actual, fixture) {
		t.Fatalf("resulting batch response doesn't match fixture:\n\nexpected:\n\n\t%q\n\n\nactual:\n\n\t%q", fixture, actual)
	}
}
