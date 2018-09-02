package server

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
	"github.com/jeffrom/logd/transport"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func BenchmarkClientBatch(b *testing.B) {
	b.SetParallelism(4)

	conf := testhelper.DefaultConfig(testing.Verbose())
	srv := NewTestServer(conf)
	rh := transport.NewMockRequestHandler(conf)
	srv.SetHandler(rh)
	srv.GoServe()
	defer CloseTestServer(b, srv, rh)

	rh.Respond(func(req *protocol.Request) *protocol.Response {
		resp := protocol.NewResponse(conf)
		cr := protocol.NewClientBatchResponse(conf, 0, 1)
		req.WriteResponse(resp, cr)
		return resp
	})

	b.ResetTimer()
	b.RunParallel(func(b *testing.PB) {
		fixture := testhelper.LoadFixture("batch.small")
		batch := protocol.NewBatch(conf)
		buf := bytes.NewBuffer(fixture)
		br := bufio.NewReader(buf)
		if _, err := batch.ReadFrom(br); err != nil {
			panic(err)
		}

		c, err := client.Dial(srv.ListenAddr().String())
		if err != nil {
			panic(err)
		}
		defer c.Close()

		for b.Next() {
			if _, err := c.Batch(batch); err != nil {
				panic(err)
			}
		}
	})
}
