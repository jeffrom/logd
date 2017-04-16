package logd

import (
	"io/ioutil"
	"log"
	"runtime/debug"
	"testing"
)

var someMessage = []byte("cool, reasonably-sized message. something about the length of an access log, or a json object displaying some information about a request. Not too big, not too small. Probably about 200 bytes, maybe more. I mean, these things are pretty arbitrary, really. In many instances, 200 bytes would be far too small. In others, too large.")

func serverBenchConfig() *Config {
	return serverBenchConfigWithOpts(true)
}

func serverBenchConfigWithOpts(discard bool) *Config {
	config := NewConfig()

	logger := newMemLogger()
	logger.discard = true
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

func BenchmarkServerStartStop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		newTestServer(serverBenchConfig()).stop()
	}
}

func BenchmarkServerConnect(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfig()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		client := newTestNetConn(config, srv)
		client.close()
	}
}

func BenchmarkServerPing(b *testing.B) {
	b.StopTimer()
	config := serverBenchConfig()
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestNetConn(config, srv)
	defer client.close()

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

	client := newTestNetConn(config, srv)
	defer client.close()

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

	client := newTestNetConn(config, srv)
	defer client.close()
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
	config := serverBenchConfigWithOpts(true)
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestNetConn(config, srv)
	defer client.close()

	writerClient := newTestNetConn(config, srv)
	defer writerClient.close()

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
	config := serverBenchConfigWithOpts(true)
	srv := newTestServer(config)
	defer closeTestServer(b, srv)

	client := newTestNetConn(config, srv)
	defer client.close()

	writerClient := newTestNetConn(config, srv)
	defer writerClient.close()

	var scanners []*Scanner
	for i := 0; i < 20; i++ {
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
