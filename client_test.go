package logd

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// some golden file tests n stuff

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

func dialTestConn(r io.Reader, w io.Writer) *Client {
	conn, err := Dial("", &testClient{Reader: r, Writer: w})
	if err != nil {
		panic(err)
	}
	return conn
}

func newMockClient(config *Config, srv *SocketServer) (*Client, *mockConn) {
	c, err := net.Dial("tcp", srv.ln.Addr().String())
	if err != nil {
		panic(err)
	}
	mc := newMockConn(c)
	client, _ := DialConfig(srv.ln.Addr().String(), config, mc)

	return client, mc
}

func TestConnProtocolWriter(t *testing.T) {
	r := &bytes.Buffer{}
	w := &bytes.Buffer{}
	config := defaultTestConfig()

	conn := dialTestConn(r, w)
	conn.writeCommand(NewCommand(config, CmdPing))
	conn.writeCommand(NewCommand(config, CmdMessage, []byte("Cool arg")))
	conn.writeCommand(NewCommand(config, CmdMessage, []byte("one arg"), []byte("two arg")))
	conn.writeCommand(NewCommand(config, CmdMessage, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")))
	conn.writeCommand(NewCommand(config, CmdRead, []byte("0"), []byte("0")))
	conn.writeCommand(NewCommand(config, CmdHead, []byte("100")))
	conn.writeCommand(NewCommand(config, CmdClose))
	conn.writeCommand(NewCommand(config, CmdShutdown))
	conn.writeCommand(NewCommand(config, CmdMessage, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")))

	err := conn.flush()
	checkError(t, err)

	checkGoldenFile(t, "conn_test", w.Bytes(), golden)
}

func TestClientWriteFails(t *testing.T) {
	config := defaultTestConfig()

	srv := newTestServer(testConfig(newMemLogger()))
	defer closeTestServer(t, srv)
	client, mockConn := newMockClient(config, srv)
	defer client.Close()

	mockConn.failWriteWith(&net.OpError{
		Source: srv.ln.Addr(),
		Net:    "tcp",
		Op:     "dial",
		Err:    errors.New("unknown host"),
	})

	_, err := client.Do(NewCommand(config, CmdPing))
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
}
