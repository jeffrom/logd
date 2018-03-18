package app_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/server"
	"github.com/jeffrom/logd/testhelper"
)

func runTest(t *testing.T, name string, f func(t *testing.T, c *config.Config), conf *config.Config) {

	t.Run(name, func(t *testing.T) {
		conf.LogFile = testhelper.TmpLog()
		f(t, conf)
	})

	// log.Printf("coverage: %.1f%% of statements", testing.Coverage()*100.00)
}

func newTestServer(conf *config.Config) *server.SocketServer {
	srv := server.NewServer(conf.Hostport, conf)
	srv.GoServe()
	return srv
}

func newTestClient(conf *config.Config, srv *server.SocketServer) *client.Client {
	conn, err := client.DialConfig(srv.ListenAddress().String(), conf)
	if err != nil {
		log.Fatal(logWithStack("%v", err))
	}
	return conn
}

func failOnError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func stopServer(t *testing.T, srv *server.SocketServer) {
	if err := srv.Stop(); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := expectNoConnections(srv); err != nil {
		t.Fatalf("%+v", err)
	}
}

func expectNoConnections(srv *server.SocketServer) error {
	if conns := srv.Conns(); len(conns) > 0 {
		return errors.New("unclosed client connections")
	}
	return nil
}

func expectRespOK(t testing.TB, resp *protocol.Response) {
	if resp.Status != protocol.RespOK {
		b, _ := resp.SprintBytes()
		t.Fatal(logWithStack("response was not OK: %q", b))
	}
}

func expectRespOKID(t testing.TB, resp *protocol.Response, id uint64) {
	if resp.Status != protocol.RespOK || resp.ID != id {
		b, _ := resp.SprintBytes()
		t.Fatal(logWithStack("Response was not OK: %q", b))
	}
}

func expectNoPendingData(t testing.TB, conf *config.Config, c *client.Client) {
	buf := make([]byte, 1024)

	c.Conn.SetReadDeadline(time.Now())
	n, _ := c.Conn.Read(buf)
	if n > 0 {
		t.Fatalf("unexpectedly read %d bytes: %q", n, buf[:n])
	}
	c.Conn.SetReadDeadline(time.Now().Add(time.Duration(conf.ClientTimeout)))
}

func expectLineMatch(t testing.TB, scanner *protocol.ProtocolScanner, msg []byte) {
	readALine := scanner.Scan()
	if !readALine {
		t.Fatal(logWithStack("Expected to scan one message but failed: %s", scanner.Error()))
	}
	if err := scanner.Error(); err != nil {
		t.Fatal(logWithStack("unexpected error scanning: %+v", err))
	}

	if respMsg := scanner.Message(); respMsg == nil || !reflect.DeepEqual(respMsg.Body, msg) {
		if respMsg == nil {
			t.Fatal(logWithStack("Expected %q response but got nil message", msg))
		} else {
			t.Fatal(logWithStack("Expected %q response but got %q", msg, respMsg.Body))
		}
	}
}

func expectLineMatchID(t testing.TB, scanner *protocol.ProtocolScanner, msg []byte, id uint64) {
	expectLineMatch(t, scanner, msg)
	m := scanner.Message()
	if m.ID != id {
		t.Fatal(logWithStack("Expected id %d but got %d", id, m.ID))
	}
}

func createMessageCommands(conf *config.Config, n int, size int) []*protocol.Command {
	if size <= n {
		panic("createMessageCommands: n must be greater than size")
	}

	var cmds []*protocol.Command
	sizeper := size / n
	written := 0

	for i := 0; written < size; i++ {
		msg := []byte(fmt.Sprintf("%d", i+1))
		for j := 0; j < sizeper && len(msg) < sizeper; j++ {
			msg = append(msg, []byte(fmt.Sprintf(" %d", i+1))...)
		}

		cmd := protocol.NewCommand(conf, protocol.CmdMessage, msg)
		cmds = append(cmds, cmd)
		written += len(msg)
	}

	return cmds
}

func writeMessages(t testing.TB, conf *config.Config, c *client.Client, cmds []*protocol.Command) {
	for i, cmd := range cmds {
		resp, err := c.Do(cmd)
		if err != nil {
			t.Fatalf("failed to write message %d: %+v", i, err)
		}
		expectRespOK(t, resp)
		expectNoPendingData(t, conf, c)
		c.Flush()
	}
}

func logWithStack(format string, args ...interface{}) string {
	stack := debug.Stack()
	parts := bytes.SplitN(stack, []byte("\n"), 1)
	newArgs := make([]interface{}, len(args))
	copy(newArgs, args)
	return fmt.Sprintf(format+"\n\n%s", append(newArgs, parts[len(parts)-1])...)
}

func expectPartitionReads(t testing.TB, conf *config.Config, c *client.Client) {
	partitions := testhelper.ListPartitions(conf)
	t.Logf("%d partitions at %s", len(partitions), filepath.Dir(conf.LogFile))
	for _, part := range partitions {
		t.Logf("checking partition at %s", part)

		first := readFirstFromFile(conf, part)
		if first != nil {
			// panic(fmt.Sprintf("%+v", first))
			scanner, err := c.DoRead(first.ID, 1)
			if err != nil {
				panic(err)
			}
			expectLineMatchID(t, scanner, first.Body, first.ID)
			expectAllScanned(t, conf, scanner)
		}
	}

}

func expectAllScanned(t testing.TB, conf *config.Config, scanner *protocol.ProtocolScanner) {
	var firstMsg *protocol.Message
	var lastMsg *protocol.Message
	for scanner.Scan() {
		if firstMsg == nil {
			firstMsg = scanner.Message()
		}
		lastMsg = scanner.Message()
	}

	if firstMsg != nil {
		t.Logf("unexpected additional message: %+v", firstMsg)
	}
	if firstMsg != lastMsg {
		t.Logf("unexpected final additional message: %+v", lastMsg)
	}

	if firstMsg != nil {
		t.Fatal("unexpected messages in scanner")
	}
}

func readFirstFromFile(conf *config.Config, fname string) *protocol.Message {
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	msg, err := protocol.MsgFromReader(f)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	return msg
}

// XXX not yet using this
func readLastFromBytes(conf *config.Config, fname string) *protocol.Message {
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	info, err := f.Stat()
	if err != nil {
		panic(err)
	}
	filelen := info.Size()

	var msg *protocol.Message
	ps := protocol.NewProtocolScanner(conf, f)
	for read := int64(0); read < filelen; {
		n, m, err := ps.ReadMessage()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			panic(err)
		}
		read += int64(n)
		msg = m
	}
	return msg
}
func TestApp(t *testing.T) {
	configs := []*config.Config{
		config.DefaultConfig,
		&config.Config{
			ServerTimeout:   500,
			ClientTimeout:   500,
			LogFileMode:     0644,
			MaxChunkSize:    1024 * 1024,
			PartitionSize:   1024 * 1024,
			IndexCursorSize: 10,
			MaxPartitions:   5,
		},
	}

	for _, conf := range configs {
		conf := conf
		conf.Verbose = testing.Verbose()

		runTest(t, "starts and stops cleanly", testServerStartStop, conf)
		runTest(t, "client can connect", testServerClientConnect, conf)
		runTest(t, "client can ping", testServerPing, conf)
		runTest(t, "client can write a message", testServerMsg, conf)
		runTest(t, "client can read a message", testServerRead, conf)
		runTest(t, "client can read from other writes", testServerReadNewWrite, conf)
		runTest(t, "client can use tail command", testServerTail, conf)
		runTest(t, "client can tail other writes", testServerTailNewWrite, conf)
		runTest(t, "client can tail from first available id", testServerTailFromTail, conf)
		runTest(t, "invalid requests", testServerInvalidRequests, conf)
		runTest(t, "writes partitions", testServerWritePartition, conf)
		runTest(t, "concurrent writes", testConcurrentWrites, conf)
	}
}

func testServerStartStop(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	stopServer(t, srv)
}

func testServerClientConnect(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	c.Close()

	if err := expectNoConnections(srv); err != nil {
		t.Fatalf("%+v", err)
	}
}

func testServerPing(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdPing))
	testhelper.CheckError(err)

	if resp.Status != protocol.RespOK {
		t.Fatalf("expected ok response, got: %+v", resp)
	}
}

func testServerMsg(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdMessage, []byte("cool message")))
	testhelper.CheckError(err)

	expectRespOKID(t, resp, 1)
}

func testServerRead(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	msg := []byte("cool message")
	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	expectRespOKID(t, resp, 1)

	scanner, err := c.DoRead(1, 1)
	testhelper.CheckError(err)

	expectLineMatch(t, scanner, msg)
}

func testServerReadNewWrite(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	readClient := newTestClient(conf, srv)
	defer readClient.Close()

	msg := []byte("cool message")
	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	expectRespOKID(t, resp, 1)

	scanner, err := readClient.DoRead(1, 0)
	testhelper.CheckError(err)
	expectLineMatch(t, scanner, msg)

	resp, err = c.Do(protocol.NewCommand(conf, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	expectRespOKID(t, resp, 2)

	expectLineMatch(t, scanner, msg)
}

func testServerTail(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	msg := []byte("cool message")
	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	expectRespOKID(t, resp, 1)

	old := conf.ReadFromTail
	conf.ReadFromTail = true
	scanner, err := c.DoRead(1, 1)
	testhelper.CheckError(err)
	conf.ReadFromTail = old

	expectLineMatch(t, scanner, msg)
}

func testServerTailNewWrite(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	readClient := newTestClient(conf, srv)
	defer readClient.Close()

	msg := []byte("cool message")
	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	expectRespOKID(t, resp, 1)

	old := conf.ReadFromTail
	conf.ReadFromTail = true
	scanner, err := readClient.DoRead(1, 0)
	testhelper.CheckError(err)
	expectLineMatch(t, scanner, msg)
	conf.ReadFromTail = old

	resp, err = c.Do(protocol.NewCommand(conf, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	expectRespOKID(t, resp, 2)

	expectLineMatch(t, scanner, msg)
}

func testServerTailFromTail(t *testing.T, conf *config.Config) {
	old := conf.LogFile
	conf.LogFile = "testdata/workofart"

	defer func() {
		conf.LogFile = old
	}()

	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	msg := []byte("Painting simply is in no position to present an object for simultaneous collective experience, as it was possible for architecture at all times, for the epic poem in the past, and for the movie today. Although this circumstance in itself should not lead one to conclusions about the social role of painting, it does constitute a serious threat as soon as painting, under special conditions and, as it were, against its nature, is confronted directly by the masses. In the churches and monasteries of the Middle Ages and at the princely courts up to the end of the eighteenth century, a collective reception of paintings did not occur simultaneously, but by graduated and hierarchized mediation. The change that has come about is an expression of the particular conflict in which painting was implicated by the mechanical reproducibility of paintings. Although paintings began to be publicly exhibited in galleries and salons, there was no way for the masses to organize and control themselves in their reception. Thus the same public which responds in a progressive manner toward a grotesque film is bound to respond in a reactionary manner to surrealism.")

	oldTail := conf.ReadFromTail
	conf.ReadFromTail = true
	scanner, err := c.DoRead(1, 1)
	testhelper.CheckError(err)
	conf.ReadFromTail = oldTail

	expectLineMatch(t, scanner, msg)
}

func testServerSleep(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdSleep, []byte("1")))
	testhelper.CheckError(err)
	expectRespOK(t, resp)
}

func testServerInvalidRequests(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	var tests = []struct {
		cmd      *protocol.Command
		expected *protocol.Response
		reason   string
	}{
		{
			protocol.NewCommand(conf, protocol.CmdMessage, []byte("")),
			protocol.NewClientErrResponse(conf, []byte("empty message not allowed")),
			"Server should not accept empty messages",
		},
		{
			protocol.NewCommand(conf, protocol.CmdMessage),
			protocol.NewClientErrResponse(conf, []byte("must supply an argument")),
			"Server should not accept missing message argument",
		},

		// TODO need to DoRead for this
		// {
		// 	NewCommand(conf, CmdRead),
		// 	NewClientErrResponse(conf, []byte("invalid request")),
		// 	"Server should not accept missing read argument",
		// },

		{
			protocol.NewCommand(conf, protocol.CmdHead, []byte("0")),
			protocol.NewClientErrResponse(conf, []byte("invalid request")),
			"Server should not accept extra head argument",
		},

		{
			protocol.NewCommand(conf, protocol.CmdPing, []byte("0")),
			protocol.NewClientErrResponse(conf, []byte("invalid request")),
			"Server should not accept extra ping argument",
		},

		{
			protocol.NewCommand(conf, protocol.CmdClose, []byte("0")),
			protocol.NewClientErrResponse(conf, []byte("invalid request")),
			"Server should not accept extra close argument",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.reason, func(t *testing.T) {
			t.Logf("\t  Sending: %s", testCase.cmd)
			t.Logf("\tExpecting: %s", testCase.expected)

			resp, err := c.Do(testCase.cmd)
			testhelper.CheckError(err)
			if resp.Status != testCase.expected.Status {
				t.Fatalf("Incorrect response type: wanted %s, got %s", testCase.expected.Status, resp.Status)
			}

			if !bytes.Equal(resp.Body, testCase.expected.Body) {
				t.Fatalf("Incorrect response body: \nwanted:\n%q, \ngot:\n%q", testCase.expected.Body, resp.Body)
			}

			t.Logf("\tOK")
		})
	}
}

func testServerWritePartition(t *testing.T, conf *config.Config) {
	cmds := createMessageCommands(conf, 500, conf.PartitionSize)

	for i := 0; i < conf.MaxPartitions; i++ {
		srv := newTestServer(conf)
		c := newTestClient(conf, srv)
		// XXX can't read and write safely from the same client yet
		c2 := newTestClient(conf, srv)

		expectPartitionReads(t, conf, c2)

		t.Logf("filling partition %d with %d messages", i, len(cmds))

		writeMessages(t, conf, c, cmds)

		part, err := ioutil.ReadFile(fmt.Sprintf("%s.%d", conf.LogFile, i))
		testhelper.CheckError(err)
		if len(part) > conf.PartitionSize {
			t.Fatalf("partition was larger than partition size: %d", conf.PartitionSize)
		}

		c2.Close()
		c2 = newTestClient(conf, srv)
		expectPartitionReads(t, conf, c2)

		// XXX we don't account for the protocol envelope when calculating
		// message sizes, so we can't be sure if there will be a later
		// partition or not

		// _, err = ioutil.ReadFile(conf.LogFile + fmt.Sprintf(".%d", i+1))
		// if err == nil {
		// 	t.Fatalf("Expected no partition %d", i+1)
		// }

		c.Close()
		c2.Close()
		stopServer(t, srv)
	}
}

func testConcurrentWrites(t *testing.T, conf *config.Config) {
	len := conf.PartitionSize / 20
	if len <= 0 {
		len = 20
	}
	if len > conf.MaxPartitions*conf.PartitionSize {
		t.Skip("skipping test as partition config is too small")
	}

	var wg sync.WaitGroup
	srv := newTestServer(conf)
	defer stopServer(t, srv)
	nwriters := 20
	msgper := 10
	total := nwriters * msgper

	for i := 0; i < nwriters; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			cmds := createMessageCommands(conf, msgper, len)
			c := newTestClient(conf, srv)
			writeMessages(t, conf, c, cmds)
			c.Close()
		}()
	}

	wg.Wait()

	c := newTestClient(conf, srv)
	scanner, err := c.DoRead(1, total)
	testhelper.CheckError(err)

	nread := 0
	for scanner.Scan() {
		nread++
	}

	if nread != total {
		t.Fatalf("expected to read %d message but only read %d", total, nread)
	}
}
