package logd

import (
	"io/ioutil"
	"log"
	"runtime/debug"
	"sync"
	"testing"
)

var someMessage = []byte("cool, reasonably-sized message. something about the length of an access log, or a json object displaying some information about a request. Not too big, not too small. Probably about 200 bytes, maybe more. I mean, these things are pretty arbitrary, really. In many instances, 200 bytes would be far too small. In others, too large.")

func serverBenchConfig() *Config {
	return serverBenchConfigWithOpts(true)
}

func serverBenchConfigWithOpts(discard bool) *Config {
	config := NewConfig()
	config.ServerTimeout = 500
	config.ClientTimeout = 500
	config.MaxChunkSize = 1024 * 10
	config.PartitionSize = 1024 * 1024 * 500
	config.IndexCursorSize = 1000

	logger := newMemLogger()
	logger.discard = discard
	// logger.returnErr = loggerShouldErr

	config.Verbose = false
	config.Logger = logger

	log.SetOutput(ioutil.Discard)
	return config
}

func startServerForBench(b *testing.B) *SocketServer {
	return startServerForBenchWithConfig(b, serverBenchConfig())
}

func startServerForBenchWithConfig(b *testing.B, config *Config) *SocketServer {
	srv := NewServer("127.0.0.1:0", config)
	if err := srv.ListenAndServe(); err != nil {
		b.Logf("%s", debug.Stack())
		b.Fatalf("error running server: %v", err)
	}
	return srv
}

// func BenchmarkServerStartStop(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		newTestServer(serverBenchConfig()).stop()
// 	}
// }

func BenchmarkServerConnect(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfig()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		client := newTestClient(config, srv)
		client.Close()
	}
}

func BenchmarkServerPing(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfig()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		client.Do(NewCommand(CmdPing))
	}
}

func BenchmarkServerMsg(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfig()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	msg := someMessage
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		client.Do(NewCommand(CmdMessage, msg))
	}
}

func BenchmarkServerRead(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfigWithOpts(false)
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	client.Do(NewCommand(CmdMessage, someMessage))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		scanner, _ := client.DoRead(1, 1)
		for scanner.Scan() {
		}
	}
}

func BenchmarkServerTail(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfig()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	writerClient := newTestClient(config, srv)
	defer writerClient.Close()

	scanner, _ := client.DoRead(1, 0)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		writerClient.Do(NewCommand(CmdMessage, someMessage))
		for scanner.Scan() {
		}
	}
}

func BenchmarkServerTailTwenty(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfig()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	writerClient := newTestClient(config, srv)
	defer writerClient.Close()

	var scanners []*Scanner
	for i := 0; i < 20; i++ {
		client := newTestClient(config, srv)
		defer client.Close()

		scanner, _ := client.DoRead(1, 0)
		scanners = append(scanners, scanner)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		writerClient.Do(NewCommand(CmdMessage, someMessage))
		for _, scanner := range scanners {
			for scanner.Scan() {
			}
		}
	}
}

func BenchmarkServerLoadTest(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfigWithOpts(true)
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	var writers []*Client
	for i := 0; i < 50; i++ {
		writerClient := newTestClient(config, srv)
		defer writerClient.Close()
		writers = append(writers, writerClient)
	}

	var scanners []*Scanner
	for i := 0; i < 50; i++ {
		client := newTestClient(config, srv)
		defer client.Close()

		scanner, _ := client.DoRead(1, 0)
		scanners = append(scanners, scanner)
	}

	var wg sync.WaitGroup
	var connwg sync.WaitGroup

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for ii := 0; ii < 50; ii++ {
			connwg.Add(1)
			go func() {
				client := newTestClient(config, srv)
				client.Close()
				connwg.Done()
			}()
		}

		for _, writerClient := range writers {
			wg.Add(1)
			go func(writerClient *Client) {
				writerClient.Do(NewCommand(CmdMessage, someMessage))
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
