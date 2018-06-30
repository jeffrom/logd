package client

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestScannerV2(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	conf.Offset = 0
	conf.Limit = 3
	gconf := conf.toGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, clientConn := testhelper.Pipe()
	defer server.Close()
	c := NewClientV2(conf).SetConn(clientConn)
	s := ScannerForClientV2(c)

	expected := []byte(fmt.Sprintf("READV2 0 %d\r\n", conf.Limit))
	server.Expect(func(p []byte) io.WriterTo {
		if !bytes.Equal(p, expected) {
			log.Panicf("expected:\n\n\t%q\n\n but got:\n\n\t%q", expected, p)
		}

		return readOKResponse(gconf, 0, fixture)
	})

	expectedMsgs := [][]byte{
		[]byte("hi"),
		[]byte("hallo"),
		[]byte("sup"),
	}

	for i, expectedMsg := range expectedMsgs {
		ok := s.Scan()
		if !ok {
			t.Fatalf("stopped scanning too early (%d/%d) (err: %+v)", i, len(expectedMsgs), s.Error())
		}

		actual := s.Message().BodyBytes()
		if !bytes.Equal(expectedMsg, actual) {
			t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q", expectedMsg, actual)
		}
	}

	if err := s.Error(); err != nil {
		t.Fatalf("scan: %+v", err)
	}

	if s.Scan() {
		t.Fatalf("did not expect to scan another batch")
	}
}
