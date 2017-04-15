package logd

import (
	"reflect"
	"runtime/debug"
	"testing"
)

func newTestNetConn(config *Config, srv *server) *Client {
	conn, err := Dial(srv.ln.Addr().String())
	if err != nil {
		panic(err)
	}
	return conn
}

func newTestServer(config *Config) *server {
	srv := newServer("127.0.0.1:0", config)
	srv.goServe()

	return srv
}

func closeTestServer(t *testing.T, srv *server) {
	srv.stop()
	srv.connMu.Lock()
	defer srv.connMu.Unlock()
	if len(srv.conns) > 0 {
		for conn := range srv.conns {
			t.Logf("Leftover connection: %s", conn.RemoteAddr())
		}
		t.Fatalf("Leftover connections after server shutdown complete")
	}
}

func checkScan(t *testing.T, scanner *protoScanner, msg []byte) {
	readALine := scanner.Scan()
	if !readALine {
		debug.PrintStack()
		t.Fatalf("Expected to scan one message but failed: %s", scanner.Err())
	}
	if err := scanner.Err(); err != nil {
		debug.PrintStack()
		t.Fatalf("unexpected error scanning: %v", err)
	}

	if respMsg := scanner.Message(); respMsg == nil || !reflect.DeepEqual(respMsg.body, msg) {
		debug.PrintStack()
		if respMsg == nil {
			t.Fatalf("Expected %q response but got nil message", msg)
		} else {
			t.Fatalf("Expected %q response but got %q", msg, respMsg.body)
		}
	}
}

func checkRespOK(t *testing.T, resp *response) {
	if !reflect.DeepEqual(resp, newResponse(respOK)) {
		debug.PrintStack()
		t.Fatalf("response was not OK: %q", resp.Bytes())
	}
}

func TestPingServer(t *testing.T) {
	srv := newTestServer(defaultTestConfig())
	defer closeTestServer(t, srv)

	client := newTestNetConn(defaultTestConfig(), srv)
	defer client.close()

	resp, err := client.do(newCommand(cmdPing))
	checkError(t, err)

	if !reflect.DeepEqual(resp, newResponse(respOK)) {
		t.Fatalf("response was not OK: %+v", resp)
	}
}

func TestMsgServer(t *testing.T) {
	srv := newTestServer(testConfig(false))
	defer closeTestServer(t, srv)

	client := newTestNetConn(defaultTestConfig(), srv)
	defer client.close()

	resp, err := client.do(newCommand(cmdMsg, []byte("cool message")))
	checkError(t, err)

	if !reflect.DeepEqual(resp, newResponse(respOK)) {
		t.Fatalf("response was not OK: %q", resp.Bytes())
	}
}

func TestReadServer(t *testing.T) {
	srv := newTestServer(testConfig(false))
	defer closeTestServer(t, srv)

	client := newTestNetConn(defaultTestConfig(), srv)
	defer client.close()

	msg := []byte("cool message")
	resp, err := client.do(newCommand(cmdMsg, msg))
	checkError(t, err)
	checkRespOK(t, resp)

	scanner, err := client.doRead(1, 1)
	checkError(t, err)

	checkScan(t, scanner, msg)
}

func TestTailServer(t *testing.T) {
	srv := newTestServer(testConfig(false))
	defer closeTestServer(t, srv)

	client := newTestNetConn(defaultTestConfig(), srv)
	defer client.close()

	writerClient := newTestNetConn(defaultTestConfig(), srv)
	defer writerClient.close()

	scanner, err := client.doRead(1, 0)
	checkError(t, err)

	msg := []byte("cool message")
	resp, err := writerClient.do(newCommand(cmdMsg, msg))
	checkError(t, err)
	checkRespOK(t, resp)

	checkScan(t, scanner, msg)

	secondTailClient := newTestNetConn(defaultTestConfig(), srv)
	defer secondTailClient.close()

	secondScanner, err := client.doRead(1, 0)
	checkError(t, err)
	checkScan(t, secondScanner, msg)

	msg = []byte("another cool message")
	resp, err = writerClient.do(newCommand(cmdMsg, msg))
	checkError(t, err)
	checkRespOK(t, resp)

	checkScan(t, scanner, msg)
	checkScan(t, secondScanner, msg)
}
