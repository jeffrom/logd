package client

import (
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func BenchmarkBatch(b *testing.B) {
	conf := DefaultTestConfig(testing.Verbose())
	gconf := conf.ToGeneralConfig()
	fixture := testhelper.LoadFixture("batch.small")
	batch := protocol.NewBatch(gconf)
	if _, err := batch.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture))); err != nil {
		panic(err)
	}
	cr := protocol.NewClientBatchResponse(gconf, 10, 1)
	c, _ := newBenchmarkConns(conf, fixture, cr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Batch(batch); err != nil {
			panic(err)
		}
	}
}

func newBenchmarkConns(conf *Config, fixture []byte, cr *protocol.ClientResponse) (*Client, func()) {
	l := len(fixture)
	server, client := net.Pipe()
	c := New(conf).SetConn(client)
	b := make([]byte, conf.BatchSize)
	stopC := make(chan struct{})

	go func() {
		for {
			select {
			case <-stopC:
				break
			default:
			}

			var read int64
			for read < int64(l) {
				n, err := server.Read(b[read:])
				read += int64(n)
				if read >= int64(l) {
					break
				}
				if err != nil {
					panic(err)
				}
			}

			if _, err := cr.WriteTo(server); err != nil {
				panic(err)
			}
		}
	}()

	return c, func() {
		stopC <- struct{}{}
		c.Close()
		server.Close()
	}
}
