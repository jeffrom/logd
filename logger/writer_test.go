package logger

import (
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

const defaultTopic = "default"

func TestWrite(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	logw := NewWriter(conf, defaultTopic)
	fixture := testhelper.LoadFixture("batch.small")

	if err := logw.Setup(); err != nil {
		t.Fatal(err)
	}

	if err := logw.SetPartition(0); err != nil {
		t.Fatalf("unexpected error setting partition: %+v", err)
	}

	n, err := logw.Write(fixture)
	if err != nil {
		t.Fatalf("unexpected error writing: %+v", err)
	}
	if n != len(fixture) {
		t.Fatalf("expected to write %d bytes, but wrote %d", len(fixture), n)
	}

	if err := logw.Flush(); err != nil {
		t.Fatalf("unexpected error flushing: %+v", err)
	}

	if err := logw.Close(); err != nil {
		t.Fatalf("unexpected error closing: %+v", err)
	}
}
