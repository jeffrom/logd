package protocol

import (
	"bytes"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestWriteClientResponse(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	resp := NewClientResponse(conf)
	resp.SetOffset(10)
	resp.SetBatches(20)
	fixture := testhelper.LoadFixture("batch_response.simple")
	b := &bytes.Buffer{}

	if _, err := resp.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing response: %+v", err)
	}

	actual := b.Bytes()
	testhelper.CheckGoldenFile("batch_response.simple", actual, testhelper.Golden)
	if !bytes.Equal(actual, fixture) {
		t.Fatalf("resulting batch response doesn't match fixture:\n\nexpected:\n\n\t%q\n\n\nactual:\n\n\t%q", fixture, actual)
	}
}
