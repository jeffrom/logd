package server

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

// some testhelper.Golden file tests n stuff

type testClient struct {
	io.Reader
	io.Writer
}

func (*testClient) Close() error                       { return nil }
func (*testClient) LocalAddr() net.Addr                { return nil }
func (*testClient) RemoteAddr() net.Addr               { return nil }
func (*testClient) SetDeadline(t time.Time) error      { return nil }
func (*testClient) SetReadDeadline(t time.Time) error  { return nil }
func (*testClient) SetWriteDeadline(t time.Time) error { return nil }

func dialTestConn(r io.Reader, w io.Writer) *client.Client {
	conn, err := client.Dial("", &testClient{Reader: r, Writer: w})
	if err != nil {
		panic(err)
	}
	return conn
}

func newMockClient(config *config.Config, srv *SocketServer) (*client.Client, *mockConn) {
	c, err := net.Dial("tcp", srv.ln.Addr().String())
	if err != nil {
		panic(err)
	}
	mc := newMockConn(c)
	cl, _ := client.DialConfig(srv.ln.Addr().String(), config, mc)

	return cl, mc
}

func TestConnProtocolWriter(t *testing.T) {
	r := &bytes.Buffer{}
	w := &bytes.Buffer{}
	config := testhelper.DefaultTestConfig(testing.Verbose())

	conn := dialTestConn(r, w)
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdPing))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdMessage, []byte("Cool arg")))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdMessage, []byte("one arg"), []byte("two arg")))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdMessage, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdRead, []byte("0"), []byte("0")))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdHead, []byte("100")))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdClose))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdShutdown))
	conn.WriteCommand(protocol.NewCommand(config, protocol.CmdMessage, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")))

	err := conn.Flush()
	testhelper.CheckError(err)

	testhelper.CheckGoldenFile("conn_test", w.Bytes(), testhelper.Golden)
}

func TestClientWriteFails(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())

	srv := NewTestServer(testhelper.TestConfig(testing.Verbose()))
	defer CloseTestServer(t, srv)
	client, mockConn := newMockClient(config, srv)
	defer client.Close()

	mockConn.failWriteWith(&net.OpError{
		Source: srv.ln.Addr(),
		Net:    "tcp",
		Op:     "dial",
		Err:    errors.New("unknown host"),
	})

	_, err := client.Do(protocol.NewCommand(config, protocol.CmdPing))
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
}
