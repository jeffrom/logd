package logd

import (
	"bytes"
	"reflect"
	"runtime/debug"
	"testing"
)

func newTestClient(config *Config, srv *SocketServer) *Client {
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

func checkScan(t *testing.T, scanner *ProtocolScanner, msg []byte) {
	readALine := scanner.Scan()
	if !readALine {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected to scan one message but failed: %s", scanner.Error())
	}
	if err := scanner.Error(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("unexpected error scanning: %+v", err)
	}

	if respMsg := scanner.Message(); respMsg == nil || !reflect.DeepEqual(respMsg.Body, msg) {
		t.Logf("%s", debug.Stack())
		if respMsg == nil {
			t.Fatalf("Expected %q response but got nil message", msg)
		} else {
			t.Fatalf("Expected %q response but got %q", msg, respMsg.Body)
		}
	}
}

func checkRespOK(t *testing.T, resp *Response) {
	if !reflect.DeepEqual(resp, newResponse(defaultTestConfig(), RespOK)) {
		t.Logf("%s", debug.Stack())
		t.Fatalf("response was not OK: %q", resp.Bytes())
	}
}

func checkRespOKID(t *testing.T, resp *Response, id uint64) {
	if resp.Status != RespOK || resp.ID != id {
		t.Fatalf("Response was not OK: %q\n%s", resp.Bytes(), debug.Stack())
	}
}

func TestStartStopServer(t *testing.T) {
	srv := newTestServer(defaultTestConfig())
	closeTestServer(t, srv)
}

func TestPingServer(t *testing.T) {
	config := defaultTestConfig()
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	client := newTestClient(config, srv)
	defer client.Close()

	resp, err := client.Do(NewCommand(config, CmdPing))
	checkError(t, err)

	if !reflect.DeepEqual(resp, newResponse(config, RespOK)) {
		t.Fatalf("response was not OK: %+v", resp)
	}
}

func TestMsgServer(t *testing.T) {
	config := testConfig(newMemLogger())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := defaultTestConfig()
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	resp, err := client.Do(NewCommand(clientConfig, CmdMessage, []byte("cool message")))
	checkError(t, err)

	checkRespOKID(t, resp, 1)
}

func TestReadServer(t *testing.T) {
	t.SkipNow()
	config := testConfig(newMemLogger())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := defaultTestConfig()
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	msg := []byte("cool message")
	resp, err := client.Do(NewCommand(clientConfig, CmdMessage, msg))
	checkError(t, err)
	checkRespOKID(t, resp, 1)

	scanner, err := client.DoRead(1, 1)
	checkError(t, err)

	checkScan(t, scanner, msg)
}

func TestTailServer(t *testing.T) {
	t.SkipNow()
	success := make(chan struct{})
	sent := make(chan struct{})
	msg := []byte("cool message")
	config := testConfig(newMemLogger())

	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := defaultTestConfig()
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	writerClient := newTestClient(clientConfig, srv)
	defer writerClient.Close()

	scanner, err := client.DoRead(1, 0)
	checkError(t, err)

	go func() {
		<-sent
		checkScan(t, scanner, msg)
		success <- struct{}{}
	}()

	resp, err := writerClient.Do(NewCommand(clientConfig, CmdMessage, msg))
	checkError(t, err)
	checkRespOKID(t, resp, 1)

	sent <- struct{}{}
	waitForChannel(t, success)

	secondTailClient := newTestClient(clientConfig, srv)
	defer secondTailClient.Close()

	secondScanner, rerr := client.DoRead(1, 0)
	checkError(t, rerr)
	checkScan(t, secondScanner, msg)

	go func() {
		<-sent
		checkScan(t, secondScanner, msg)
		success <- struct{}{}
	}()

	msg = []byte("another cool message")
	resp, err = writerClient.Do(NewCommand(clientConfig, CmdMessage, msg))
	checkError(t, err)
	checkRespOKID(t, resp, 2)

	sent <- struct{}{}
	waitForChannel(t, success)

	go func() {
		<-sent
		checkScan(t, scanner, msg)
		success <- struct{}{}
	}()
	sent <- struct{}{}
	waitForChannel(t, success)
}

func TestServerSleep(t *testing.T) {
	config := testConfig(newMemLogger())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := defaultTestConfig()
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	resp, err := client.Do(NewCommand(clientConfig, CmdSleep, []byte("10")))
	checkError(t, err)
	checkRespOK(t, resp)
}

// func TestServerReplicate(t *testing.T) {
// 	srv := newTestServer(testConfig(newMemLogger()))
// 	defer closeTestServer(t, srv)

// 	client := newTestClient(defaultTestConfig(), srv)
// 	defer client.Close()

// 	// resp, err := client.Do(NewCommand(CmdSleep, []byte("10")))
// 	// checkError(t, err)
// 	// checkRespOK(t, resp)
// }

func TestServerInvalidRequests(t *testing.T) {
	config := testConfig(newMemLogger())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := defaultTestConfig()
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	var tests = []struct {
		cmd      *Command
		expected *Response
		reason   string
	}{
		{
			NewCommand(clientConfig, CmdMessage, []byte("")),
			NewClientErrResponse(clientConfig, []byte("empty message not allowed")),
			"Server should not accept empty messages",
		},
		{
			NewCommand(clientConfig, CmdMessage),
			NewClientErrResponse(clientConfig, []byte("must supply an argument")),
			"Server should not accept missing message argument",
		},

		{
			NewCommand(clientConfig, CmdRead),
			NewClientErrResponse(clientConfig, []byte("invalid request")),
			"Server should not accept missing read argument",
		},

		{
			NewCommand(clientConfig, CmdHead, []byte("0")),
			NewClientErrResponse(clientConfig, []byte("invalid request")),
			"Server should not accept extra head argument",
		},

		{
			NewCommand(clientConfig, CmdPing, []byte("0")),
			NewClientErrResponse(clientConfig, []byte("invalid request")),
			"Server should not accept extra ping argument",
		},

		{
			NewCommand(clientConfig, CmdClose, []byte("0")),
			NewClientErrResponse(clientConfig, []byte("invalid request")),
			"Server should not accept extra close argument",
		},
	}

	for _, testCase := range tests {
		t.Log(testCase.reason)
		t.Logf("\t  Sending: %s", testCase.cmd)
		t.Logf("\tExpecting: %s", testCase.expected)

		resp, err := client.Do(testCase.cmd)
		checkError(t, err)
		if resp.Status != testCase.expected.Status {
			t.Fatalf("Incorrect response type: wanted %s, got %s", testCase.expected.Status, resp.Status)
		}

		if !bytes.Equal(resp.body, testCase.expected.body) {
			t.Fatalf("Incorrect response body: \nwanted:\n%q, \ngot:\n%q", testCase.expected.body, resp.body)
		}
	}
}
