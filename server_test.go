package logd

import (
	"reflect"
	"runtime/debug"
	"testing"
)

func newTestNetConn(config *Config, srv *SocketServer) *Client {
	if config == nil {
		config = defaultTestConfig()
	}
	conn, err := DialConfig(srv.ln.Addr().String(), config)
	if err != nil {
		panic(err)
	}
	return conn
}

func newTestServer(config *Config) *SocketServer {
	srv := NewServer("127.0.0.1:0", config)
	srv.goServe()

	return srv
}

func closeTestServer(t testing.TB, srv *SocketServer) {
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

func checkScan(t *testing.T, scanner *Scanner, msg []byte) {
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

func checkRespOK(t *testing.T, resp *Response) {
	if !reflect.DeepEqual(resp, newResponse(respOK)) {
		debug.PrintStack()
		t.Fatalf("response was not OK: %q", resp.Bytes())
	}
}

func TestStartStopServer(t *testing.T) {
	srv := newTestServer(defaultTestConfig())
	closeTestServer(t, srv)
}

func TestPingServer(t *testing.T) {
	srv := newTestServer(defaultTestConfig())
	defer closeTestServer(t, srv)

	client := newTestNetConn(defaultTestConfig(), srv)
	defer client.close()

	resp, err := client.Do(NewCommand(CmdPing))
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

	resp, err := client.Do(NewCommand(CmdMessage, []byte("cool message")))
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
	resp, err := client.Do(NewCommand(CmdMessage, msg))
	checkError(t, err)
	checkRespOK(t, resp)

	scanner, err := client.DoRead(1, 1)
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

	scanner, err := client.DoRead(1, 0)
	checkError(t, err)

	msg := []byte("cool message")
	resp, err := writerClient.Do(NewCommand(CmdMessage, msg))
	checkError(t, err)
	checkRespOK(t, resp)

	checkScan(t, scanner, msg)

	secondTailClient := newTestNetConn(defaultTestConfig(), srv)
	defer secondTailClient.close()

	secondScanner, err := client.DoRead(1, 0)
	checkError(t, err)
	checkScan(t, secondScanner, msg)

	msg = []byte("another cool message")
	resp, err = writerClient.Do(NewCommand(CmdMessage, msg))
	checkError(t, err)
	checkRespOK(t, resp)

	checkScan(t, scanner, msg)
	checkScan(t, secondScanner, msg)
}

func TestServerSleep(t *testing.T) {
	srv := newTestServer(testConfig(false))
	defer closeTestServer(t, srv)

	client := newTestNetConn(defaultTestConfig(), srv)
	defer client.close()

	resp, err := client.Do(NewCommand(CmdSleep, []byte("10")))
	checkError(t, err)
	checkRespOK(t, resp)
}
