package server

import (
	"bytes"
	"flag"
	"reflect"
	"runtime/debug"
	"testing"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

func newTestClient(config *config.Config, srv *SocketServer) *client.Client {
	if config == nil {
		config = testhelper.DefaultTestConfig(testing.Verbose())
	}

	conn, err := client.DialConfig(srv.ln.Addr().String(), config)
	if err != nil {
		panic(err)
	}
	return conn
}

func newTestServer(config *config.Config) *SocketServer {
	srv := NewServer("127.0.0.1:0", config)
	srv.goServe()

	return srv
}

func closeTestServer(t testing.TB, srv *SocketServer) {
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

func checkScan(t *testing.T, scanner *protocol.ProtocolScanner, msg []byte) {
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

func checkRespOK(t *testing.T, resp *protocol.Response) {
	if resp.Status != protocol.RespOK {
		t.Fatalf("response was not OK: %q", resp.Bytes())
	}
}

func checkRespOKID(t *testing.T, resp *protocol.Response, id uint64) {
	if resp.Status != protocol.RespOK || resp.ID != id {
		t.Fatalf("Response was not OK: %q\n%s", resp.Bytes(), debug.Stack())
	}
}

func TestStartStopServer(t *testing.T) {
	srv := newTestServer(testhelper.DefaultTestConfig(testing.Verbose()))
	closeTestServer(t, srv)
}

func TestPingServer(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	c := newTestClient(config, srv)
	defer c.Close()

	resp, err := c.Do(protocol.NewCommand(config, protocol.CmdPing))
	testhelper.CheckError(err)

	if resp.Status != protocol.RespOK {
		t.Fatalf("response was not OK: %+v", resp)
	}
}

func TestMsgServer(t *testing.T) {
	config := testhelper.TestConfig(testing.Verbose())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := testhelper.DefaultTestConfig(testing.Verbose())
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	resp, err := client.Do(protocol.NewCommand(clientConfig, protocol.CmdMessage, []byte("cool message")))
	testhelper.CheckError(err)

	checkRespOKID(t, resp, 1)
}

func TestReadServer(t *testing.T) {
	// t.SkipNow()
	config := testhelper.TestConfig(testing.Verbose())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := testhelper.DefaultTestConfig(testing.Verbose())
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	msg := []byte("cool message")
	resp, err := client.Do(protocol.NewCommand(clientConfig, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	checkRespOKID(t, resp, 1)

	scanner, err := client.DoRead(1, 1)
	testhelper.CheckError(err)

	checkScan(t, scanner, msg)
}

func TestTailServer(t *testing.T) {
	t.SkipNow()
	success := make(chan struct{})
	sent := make(chan struct{})
	msg := []byte("cool message")
	config := testhelper.TestConfig(testing.Verbose())

	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := testhelper.DefaultTestConfig(testing.Verbose())
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	writerClient := newTestClient(clientConfig, srv)
	defer writerClient.Close()

	scanner, err := client.DoRead(1, 0)
	testhelper.CheckError(err)

	go func() {
		select {
		case <-sent:
		}

		checkScan(t, scanner, msg)

		select {
		case success <- struct{}{}:
		}
	}()

	resp, err := writerClient.Do(protocol.NewCommand(clientConfig, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	checkRespOKID(t, resp, 1)

	select {
	case sent <- struct{}{}:
	}

	testhelper.WaitForChannel(success)

	secondTailClient := newTestClient(clientConfig, srv)
	defer secondTailClient.Close()

	secondScanner, rerr := client.DoRead(1, 0)
	testhelper.CheckError(rerr)
	checkScan(t, secondScanner, msg)

	go func() {
		select {
		case <-sent:
		}

		checkScan(t, secondScanner, msg)

		select {
		case success <- struct{}{}:
		}
	}()

	msg = []byte("another cool message")
	resp, err = writerClient.Do(protocol.NewCommand(clientConfig, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	checkRespOKID(t, resp, 2)

	select {
	case sent <- struct{}{}:
	}

	testhelper.WaitForChannel(success)

	go func() {
		select {
		case <-sent:
		}

		checkScan(t, scanner, msg)

		select {
		case success <- struct{}{}:
		}
	}()

	select {
	case sent <- struct{}{}:
	}

	testhelper.WaitForChannel(success)
}

func TestServerSleep(t *testing.T) {
	config := testhelper.TestConfig(testing.Verbose())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := testhelper.DefaultTestConfig(testing.Verbose())
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	resp, err := client.Do(protocol.NewCommand(clientConfig, protocol.CmdSleep, []byte("10")))
	testhelper.CheckError(err)
	checkRespOK(t, resp)
}

// func TestServerReplicate(t *testing.T) {
// 	srv := newTestServer(testhelper.TestConfig(testhelper.NewMemLogger()))
// 	defer closeTestServer(t, srv)

// 	client := newTestClient(testhelper.DefaultTestConfig(), srv)
// 	defer client.Close()

// 	// resp, err := client.Do(NewCommand(CmdSleep, []byte("10")))
// 	// testhelper.CheckError(t, err)
// 	// checkRespOK(t, resp)
// }

func TestServerInvalidRequests(t *testing.T) {
	// t.SkipNow()
	config := testhelper.TestConfig(testing.Verbose())
	srv := newTestServer(config)
	defer closeTestServer(t, srv)

	clientConfig := testhelper.DefaultTestConfig(testing.Verbose())
	client := newTestClient(clientConfig, srv)
	defer client.Close()

	var tests = []struct {
		cmd      *protocol.Command
		expected *protocol.Response
		reason   string
	}{
		{
			protocol.NewCommand(clientConfig, protocol.CmdMessage, []byte("")),
			protocol.NewClientErrResponse(clientConfig, []byte("empty message not allowed")),
			"Server should not accept empty messages",
		},
		{
			protocol.NewCommand(clientConfig, protocol.CmdMessage),
			protocol.NewClientErrResponse(clientConfig, []byte("must supply an argument")),
			"Server should not accept missing message argument",
		},

		// TODO need to DoRead for this
		// {
		// 	NewCommand(clientConfig, CmdRead),
		// 	NewClientErrResponse(clientConfig, []byte("invalid request")),
		// 	"Server should not accept missing read argument",
		// },

		{
			protocol.NewCommand(clientConfig, protocol.CmdHead, []byte("0")),
			protocol.NewClientErrResponse(clientConfig, []byte("invalid request")),
			"Server should not accept extra head argument",
		},

		{
			protocol.NewCommand(clientConfig, protocol.CmdPing, []byte("0")),
			protocol.NewClientErrResponse(clientConfig, []byte("invalid request")),
			"Server should not accept extra ping argument",
		},

		{
			protocol.NewCommand(clientConfig, protocol.CmdClose, []byte("0")),
			protocol.NewClientErrResponse(clientConfig, []byte("invalid request")),
			"Server should not accept extra close argument",
		},
	}

	for _, testCase := range tests {
		t.Log(testCase.reason)
		t.Logf("\t  Sending: %s", testCase.cmd)
		t.Logf("\tExpecting: %s", testCase.expected)

		resp, err := client.Do(testCase.cmd)
		testhelper.CheckError(err)
		if resp.Status != testCase.expected.Status {
			t.Fatalf("Incorrect response type: wanted %s, got %s", testCase.expected.Status, resp.Status)
		}

		if !bytes.Equal(resp.Body, testCase.expected.Body) {
			t.Fatalf("Incorrect response body: \nwanted:\n%q, \ngot:\n%q", testCase.expected.Body, resp.Body)
		}

		t.Logf("\tOK")
	}
}
