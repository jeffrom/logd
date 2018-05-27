package logger

import (
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestWriteV2(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	logw := NewWriter(conf)
	fixture := testhelper.LoadFixture("batch.small")

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
