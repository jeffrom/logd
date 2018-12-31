package logd

import (
	"bufio"
	"bytes"
	"io"
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
	c, _ := batchBenchmark(conf, fixture, cr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Batch(batch); err != nil {
			panic(err)
		}
	}
}

func batchBenchmark(conf *Config, fixture []byte, cr *protocol.ClientResponse) (*Client, func()) {
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

func BenchmarkRead(b *testing.B) {
	conf := DefaultTestConfig(testing.Verbose())
	c, shutdown := readBenchmark(conf)
	topic := []byte("default")

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, bs, err := c.ReadOffset(topic, 0, 3)
		if err != nil {
			b.Fatal(err)
		}

		for bs.Scan() {
		}

		if err := bs.Error(); err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}

	if err := shutdown(); err != nil {
		b.Fatal(err)
	}
}

func readBenchmark(conf *Config) (*Client, func() error) {
	gconf := conf.ToGeneralConfig()
	server, client := net.Pipe()
	c := New(conf).SetConn(client)
	done := make(chan struct{}, 1)
	errC := make(chan error, 1)
	b := make([]byte, conf.BatchSize)
	cr := protocol.NewClientBatchResponse(gconf, 0, 1)
	buf := &bytes.Buffer{}

	fixture := testhelper.LoadFixture("batch.small")
	batch := protocol.NewBatch(gconf)
	if _, err := batch.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture))); err != nil {
		panic(err)
	}

	if _, err := cr.WriteTo(buf); err != nil {
		panic(err)
	}
	if _, err := batch.WriteTo(buf); err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case <-done:
				break
			default:
			}

			_, err := server.Read(b)
			if err != nil {
				errC <- err
				return
			}

			if _, err := server.Write(buf.Bytes()); err != nil {
				errC <- err
				return
			}
		}
	}()

	return c, func() error {
		done <- struct{}{}
		server.Close()
		select {
		case err := <-errC:
			return err
		default:
		}
		return nil
	}
}
