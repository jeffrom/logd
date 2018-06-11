package client

import (
	"net"
	"testing"

	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestWriterV2(t *testing.T) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.toGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	server, w, shutdown := newTestWriterConn(conf)
	defer shutdown()

	check := goExpectResps(t, conf, server, []expectedRequest{
		{fixture, protocol.NewClientBatchResponseV2(gconf, 10)},
	}...)
	defer check()

	writeBatch(t, w, "hi", "hallo", "sup")
	flushBatch(t, w)
}

// func TestWriterFillBatchV2(t *testing.T) {
// 	conf := DefaultTestConfig(testing.Verbose())
// 	// gconf := conf.toGeneralConfig()
// 	server, w, shutdown := newTestWriterConn(conf)
// 	defer shutdown()
// 	stop := goRespond(t, conf, server)
// 	defer stop()

// 	n := 0
// 	msg := []byte("pretty cool message!")
// 	for n < conf.BatchSize-len(msg) {
// 		x, err := w.Write(msg)
// 		n += x
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// }

func writeBatch(t *testing.T, w *Writer, msgs ...string) {
	for _, msg := range msgs {
		t.Logf("write: %q", msg)
		n, err := w.Write([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		if n != len(msg) {
			t.Fatalf("expected to write %d bytes (%q), but wrote %d", len(msg), msg, n)
		}
	}
}

func flushBatch(t *testing.T, w *Writer) {
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func confForTimerTest(conf *Config) *Config {
	conf.WaitInterval = 10
	return conf
}

func newTestWriterConn(conf *Config) (net.Conn, *Writer, func()) {
	server, client := net.Pipe()
	c := NewClientV2(conf).SetConn(client)
	w := ForClientV2(c)

	return server, w, func() {
		w.Close()
		server.Close()
	}
}
