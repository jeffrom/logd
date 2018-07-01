package server

import (
	"flag"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

func NewTestServer(conf *config.Config) *Socket {
	srv := NewSocket("127.0.0.1:0", conf)
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

// func TestBatch(t *testing.T) {
// 	conf := testhelper.DefaultTestConfig(testing.Verbose())
// 	srv := NewTestServer(conf)
// 	defer CloseTestServer(t, srv)
// }
