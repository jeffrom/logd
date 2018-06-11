package server

import (
	"io/ioutil"
	"log"
	"sync"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

var someMessage = []byte("cool, reasonably-sized message. something about the length of an access log, or a json object displaying some information about a request. Not too big, not too small. Probably about 200 bytes, maybe more. I mean, these things are pretty arbitrary, really. In many instances, 200 bytes would be far too small. In others, too large.")

func serverBenchConfig(t testing.TB) *config.Config {
	return serverBenchConfigWithOpts(t, true)
}

func serverBenchConfigWithOpts(t testing.TB, discard bool) *config.Config {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	conf := config.NewConfig()
	conf.ServerTimeout = 1000
	conf.ClientTimeout = 1000
	conf.GracefulShutdownTimeout = 1000
	conf.MaxBatchSize = 1024 * 10
	// conf.PartitionSize = 1024 * 1024 * 500
	// conf.IndexCursorSize = 1000
	conf.PartitionSize = 1024 * 500
	conf.IndexCursorSize = 100
	conf.LogFileMode = 0644
	conf.MaxPartitions = 5

	// _, _, teardown := logger.SetupTestFileLoggerConfig(conf, testing.Verbose())
	conf.LogFile = testhelper.TmpLog()

	conf.Verbose = testing.Verbose()

	conf.ClientBatchSize = 10
	conf.ClientWaitInterval = 200

	return conf
}

// func startServerForBench(b *testing.B) *SocketServer {
// 	return startServerForBenchWithConfig(b, serverBenchConfig())
// }

// func startServerForBenchWithConfig(b *testing.B, config *config.Config) *SocketServer {
// 	srv := NewServer("127.0.0.1:0", config)
// 	if err := srv.ListenAndServe(); err != nil {
// 		b.Logf("%s", debug.Stack())
// 		b.Fatalf("error running server: %v", err)
// 	}
// 	return srv
// }

// func BenchmarkServerStartStop(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		newTestServer(serverBenchConfig()).stop()
// 	}
// }

func BenchmarkServerConnect(b *testing.B) {
	conf := serverBenchConfig(b)
	// fmt.Printf("config: %s\n", conf)
	srv := NewTestServer(conf)
	defer CloseTestServer(b, srv)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := newTestClient(conf, srv)
		c.Close()
	}
}

func BenchmarkServerPing(b *testing.B) {
	config := serverBenchConfig(b)
	srv := NewTestServer(config)
	defer CloseTestServer(b, srv)

	c := newTestClient(config, srv)
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Do(protocol.NewCommand(config, protocol.CmdPing))
	}
}

func BenchmarkServerMsg(b *testing.B) {
	config := serverBenchConfig(b)
	srv := NewTestServer(config)
	defer CloseTestServer(b, srv)

	c := newTestClient(config, srv)
	defer c.Close()

	msg := someMessage
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Do(protocol.NewCommand(config, protocol.CmdMessage, msg))
	}
}

func BenchmarkServerMsgBatched(b *testing.B) {
	config := serverBenchConfig(b)
	srv := NewTestServer(config)
	defer CloseTestServer(b, srv)

	l := newTestClientLogger(config, srv)
	defer l.Close()

	msg := someMessage
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Write(msg)
	}
}

func BenchmarkServerRead(b *testing.B) {
	config := serverBenchConfig(b)
	srv := NewTestServer(config)
	defer CloseTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	client.Do(protocol.NewCommand(config, protocol.CmdMessage, someMessage))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner, err := client.DoRead(1, 1)
		if err != nil {
			b.Fatalf("failed to start scanning: %+v", err)
		}
		for scanner.Scan() {

		}
		if err := scanner.Error(); err != nil {
			panic(err)
		}
	}
}

func BenchmarkServerTail(b *testing.B) {
	config := serverBenchConfig(b)
	srv := NewTestServer(config)
	defer CloseTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	writerClient := newTestClient(config, srv)
	defer writerClient.Close()

	if resp, err := writerClient.Do(protocol.NewCommand(config, protocol.CmdMessage, someMessage)); err != nil {
		panic(err)
	} else if resp.Status != protocol.RespOK {
		log.Panicf("expected ok response but got %s", resp)
	}

	for n := 0; n < config.ClientBatchSize; n++ {
		if resp, err := writerClient.Do(protocol.NewCommand(config, protocol.CmdMessage, someMessage)); err != nil {
			panic(err)
		} else if resp.Status != protocol.RespOK {
			log.Panicf("expected ok response but got %s", resp)
		}

	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner, err := client.DoRead(1, config.ClientBatchSize)
		if err != nil {
			panic(err)
		}

		for scanner.Scan() {

		}
		if err := scanner.Error(); err != nil {
			panic(err)
		}
	}
}

func BenchmarkServerTailTen(b *testing.B) {
	total := 10
	config := serverBenchConfig(b)
	srv := NewTestServer(config)
	defer CloseTestServer(b, srv)

	writerClient := newTestClient(config, srv)
	defer writerClient.Close()

	cmd := protocol.NewCommand(config, protocol.CmdMessage, someMessage)
	if resp, err := writerClient.Do(cmd); err != nil {
		panic(err)
	} else if resp.Status != protocol.RespOK {
		log.Panicf("expected ok response but got %s", resp)
	}

	var clients []*client.Client
	for i := 0; i < total; i++ {
		client := newTestClient(config, srv)
		defer client.Close()
		clients = append(clients, client)

	}

	for n := 0; n < config.ClientBatchSize; n++ {
		if resp, err := writerClient.Do(cmd); err != nil {
			panic(err)
		} else if resp.Status != protocol.RespOK {
			log.Panicf("expected ok response but got %s", resp)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, client := range clients {
			scanner, err := client.DoRead(1, config.ClientBatchSize)
			if err != nil {
				panic(err)
			}
			for scanner.Scan() {

			}
			if serr := scanner.Error(); serr != nil {
				panic(serr)
			}
		}
	}
}

func BenchmarkServerLoadTest(b *testing.B) {
	b.SkipNow()
	total := 25
	b.StopTimer()
	config := serverBenchConfig(b)
	srv := NewTestServer(config)
	defer CloseTestServer(b, srv)

	c := newTestClient(config, srv)
	defer c.Close()

	var writers []*client.Client
	for i := 0; i < total; i++ {
		writerClient := newTestClient(config, srv)
		defer writerClient.Close()
		writers = append(writers, writerClient)
	}

	writers[0].Do(protocol.NewCommand(config, protocol.CmdMessage, someMessage))

	var scanners []*client.Scanner
	for i := 0; i < total; i++ {
		c := newTestClient(config, srv)
		defer c.Close()

		scanner, _ := c.DoRead(1, 0)
		scanners = append(scanners, scanner)
	}

	var wg sync.WaitGroup
	var connwg sync.WaitGroup

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for ii := 0; ii < total; ii++ {
			connwg.Add(1)
			go func() {
				c := newTestClient(config, srv)
				c.Close()
				connwg.Done()
			}()
		}

		for _, writerClient := range writers {
			wg.Add(1)
			go func(writerClient *client.Client) {
				writerClient.Do(protocol.NewCommand(config, protocol.CmdMessage, someMessage))
				wg.Done()
			}(writerClient)
		}
		wg.Wait()

		for _, scanner := range scanners {
			for scanner.Scan() {
			}
		}

		connwg.Wait()
	}
}
