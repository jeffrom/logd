package server

import (
	"flag"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

func newTestClient(config *config.Config, srv *Socket) *client.Client {
	if config == nil {
		config = testhelper.DefaultTestConfig(testing.Verbose())
	}

	conn, err := client.DialConfig(srv.ln.Addr().String(), config)
	if err != nil {
		panic(err)
	}
	return conn
}

func NewTestServer(config *config.Config) *Socket {
	srv := NewSocket("127.0.0.1:0", config)
	srv.GoServe()

	return srv
}

func CloseTestServer(t testing.TB, srv *Socket) {
	srv.Stop()
	srv.connMu.Lock()
	defer srv.connMu.Unlock()
	if len(srv.conns) > 0 {
		for conn := range srv.conns {
			t.Logf("Leftover connection: %s", conn.RemoteAddr())
		}
		t.Fatalf("Leftover connections after server shutdown complete")
	}
}
