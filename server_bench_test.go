package logd

import (
	"io/ioutil"
	"log"
	"sync"
	"testing"
)

var someMessage = []byte("cool, reasonably-sized message. something about the length of an access log, or a json object displaying some information about a request. Not too big, not too small. Probably about 200 bytes, maybe more. I mean, these things are pretty arbitrary, really. In many instances, 200 bytes would be far too small. In others, too large.")

func serverBenchConfig(t testing.TB) (*Config, func()) {
	return serverBenchConfigWithOpts(t, true)
}

func serverBenchConfigWithOpts(t testing.TB, discard bool) (*Config, func()) {
	config := NewConfig()
	config.ServerTimeout = 500
	config.ClientTimeout = 500
	config.MaxChunkSize = 1024 * 10
	// config.PartitionSize = 1024 * 1024 * 500
	// config.IndexCursorSize = 1000
	config.PartitionSize = 1024 * 500
	config.IndexCursorSize = 100
	config.LogFileMode = 0644

	_, logger, teardown := setupFileLoggerConfig(t, config)
	config.LogFile = tmpLog()

	config.Verbose = false
	config.Logger = logger

	log.SetOutput(ioutil.Discard)
	return config, teardown
}

// func startServerForBench(b *testing.B) *SocketServer {
// 	return startServerForBenchWithConfig(b, serverBenchConfig())
// }

// func startServerForBenchWithConfig(b *testing.B, config *Config) *SocketServer {
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
	b.StopTimer()
	config, teardown := serverBenchConfig(b)
	defer teardown()
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
	config, teardown := serverBenchConfig(b)
	defer teardown()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		client.Do(NewCommand(config, CmdPing))
	}
}

func BenchmarkServerMsg(b *testing.B) {
	b.StopTimer()
	config, teardown := serverBenchConfig(b)
	defer teardown()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	msg := someMessage
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		client.Do(NewCommand(config, CmdMessage, msg))
	}
}

func BenchmarkServerRead(b *testing.B) {
	b.StopTimer()
	config, teardown := serverBenchConfig(b)
	defer teardown()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	client.Do(NewCommand(config, CmdMessage, someMessage))
	client.Close()

	for i := 0; i < b.N; i++ {
		client = newTestClient(config, srv)
		b.StartTimer()

		scanner, err := client.DoRead(1, 1)
		if err != nil {
			b.Fatalf("failed to start scanning: %+v", err)
		}
		for scanner.Scan() {
		}

		b.StopTimer()
		client.Close()
	}
}

func BenchmarkServerTail(b *testing.B) {
	b.StopTimer()
	config, teardown := serverBenchConfig(b)
	defer teardown()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	writerClient := newTestClient(config, srv)
	defer writerClient.Close()

	scanner, _ := client.DoRead(1, 0)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		writerClient.Do(NewCommand(config, CmdMessage, someMessage))
		for scanner.Scan() {
		}
	}
}

func BenchmarkServerTailTen(b *testing.B) {
	total := 10
	b.StopTimer()
	config, teardown := serverBenchConfig(b)
	defer teardown()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	writerClient := newTestClient(config, srv)
	defer writerClient.Close()

	var scanners []*ProtocolScanner
	for i := 0; i < total; i++ {
		client := newTestClient(config, srv)
		defer client.Close()

		scanner, _ := client.DoRead(1, 0)
		scanners = append(scanners, scanner)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		writerClient.Do(NewCommand(config, CmdMessage, someMessage))
		for _, scanner := range scanners {
			for scanner.Scan() {
			}
		}
	}
}

func BenchmarkServerLoadTest(b *testing.B) {
	// b.SkipNow()
	total := 25
	b.StopTimer()
	config, teardown := serverBenchConfig(b)
	defer teardown()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	var writers []*Client
	for i := 0; i < total; i++ {
		writerClient := newTestClient(config, srv)
		defer writerClient.Close()
		writers = append(writers, writerClient)
	}

	var scanners []*ProtocolScanner
	for i := 0; i < total; i++ {
		client := newTestClient(config, srv)
		defer client.Close()

		scanner, _ := client.DoRead(1, 0)
		scanners = append(scanners, scanner)
	}

	var wg sync.WaitGroup
	var connwg sync.WaitGroup

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for ii := 0; ii < total; ii++ {
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
				writerClient.Do(NewCommand(config, CmdMessage, someMessage))
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
