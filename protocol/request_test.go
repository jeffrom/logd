package protocol

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestReadRequest(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	req := NewRequest(conf)
	fixture := testhelper.LoadFixture("batch.small")

	n, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture)))
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	if n != int64(len(fixture)) {
		t.Fatalf("fixture was %d bytes but request read %d", len(fixture), n)
	}

	actual := req.raw[:req.read]
	if !bytes.Equal(actual, fixture) {
		t.Fatalf("resulting request raw bytes don't match fixture:\n\nexpected:\n\n\t%q\n\n\nactual:\n\n\t%q", fixture, actual)
	}
}
