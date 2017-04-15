package logd

import (
	"bytes"
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

func TestConnProtocolWriter(t *testing.T) {
	r := &bytes.Buffer{}
	w := &bytes.Buffer{}
	conn := dialTestConn(r, w)
	conn.writeCommand(newCommand(cmdPing))
	conn.writeCommand(newCommand(cmdMsg, []byte("Cool arg")))
	conn.writeCommand(newCommand(cmdMsg, []byte("one arg"), []byte("two arg")))
	conn.writeCommand(newCommand(cmdMsg, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")))
	conn.writeCommand(newCommand(cmdRead, []byte("0"), []byte("0")))
	conn.writeCommand(newCommand(cmdHead, []byte("100")))
	conn.writeCommand(newCommand(cmdClose))
	conn.writeCommand(newCommand(cmdShutdown))
	conn.writeCommand(newCommand(cmdMsg, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")))

	err := conn.flush()
	checkError(t, err)

	checkGoldenFile(t, "conn_test", w.Bytes(), golden)
}
