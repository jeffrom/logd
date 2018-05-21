package app_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/server"
	"github.com/jeffrom/logd/testhelper"
)

var someMessage = []byte("cool, reasonably-sized message. something about the length of an access log, or a json object displaying some information about a request. Not too big, not too small. Probably about 200 bytes, maybe more. I mean, these things are pretty arbitrary, really. In many instances, 200 bytes would be far too small. In others, too large.")

func runTest(t *testing.T, name string, f func(t *testing.T, c *config.Config), conf *config.Config) {

	t.Run(name, func(t *testing.T) {
		conf.LogFile = testhelper.TmpLog()
		f(t, conf)
	})

	// log.Printf("coverage: %.1f%% of statements", testing.Coverage()*100.00)
}

func newTestServer(conf *config.Config) *server.Socket {
	srv := server.NewSocket(conf.Hostport, conf)
	srv.GoServe()
	return srv
}

func newTestClient(conf *config.Config, srv *server.Socket) *client.Client {
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

func stopServer(t *testing.T, srv *server.Socket) {
	if err := srv.Stop(); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := expectNoConnections(srv); err != nil {
		t.Fatalf("%+v", err)
	}
}

func waitForNoConnections(srv *server.Socket) error {
	errC := make(chan error)
	go func() {
		if conns := srv.Conns(); len(conns) > 0 {
			errC <- errors.New("unclosed client connections")
		} else {
			errC <- nil
		}
	}()

	select {
	case err := <-errC:
		return err
	// TODO: graceful shutdown timeout
	case <-time.After(1000 * time.Millisecond):
		return errors.New("timed out waiting for connections to close")
	}
}

func expectNoConnections(srv *server.Socket) error {
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

func expectLineMatch(t testing.TB, scanner *client.Scanner, msg []byte) {
	readALine := scanner.Scan()
	if !readALine {
		t.Fatal(logWithStack("Expected to scan one message but failed: %s", scanner.Error()))
	}
	if err := scanner.Error(); err != nil {
		t.Fatal(logWithStack("unexpected error scanning: %+v", err))
	}

	if respMsg := scanner.Message(); respMsg == nil || !reflect.DeepEqual(respMsg.Body, msg) {
		if respMsg == nil {
			t.Fatal(logWithStack("Expected\n\n\t%q\n\nbut got\n\n\tnil", msg))
		} else {
			t.Fatal(logWithStack("Expected\n\n\t%q\n\nbut got\n\n\t%q", msg, respMsg.Body))
		}
	}
}

func expectLineMatchID(t testing.TB, scanner *client.Scanner, msg []byte, id uint64) {
	expectLineMatch(t, scanner, msg)
	m := scanner.Message()
	if m.ID != id {
		t.Fatal(logWithStack("Expected id %d but got %d", id, m.ID))
	}
}

func expectLineID(t testing.TB, scanner *client.Scanner, id uint64) {
	readALine := scanner.Scan()
	if !readALine {
		t.Fatal(logWithStack("Expected to scan one message but failed: %s", scanner.Error()))
	}
	if err := scanner.Error(); err != nil {
		t.Fatal(logWithStack("unexpected error scanning: %+v", err))
	}

	m := scanner.Message()
	if m.ID != id {
		t.Fatal(logWithStack("Expected id %d but got %d", id, m.ID))
	}
}

func expectNoLineRead(t testing.TB, scanner *client.Scanner) {
	readALine := scanner.Scan()
	if readALine {
		t.Fatal("expected not to read a line")
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

func writeMessagesUntilDone(t testing.TB, conf *config.Config, c *client.Client, cmds []*protocol.Command) func() {
	done := make(chan struct{})
	var i uint32

	writeMessages(t, conf, c, cmds)
	atomic.AddUint32(&i, 1)

	go func() {
		for {
			writeMessages(t, conf, c, cmds)
			atomic.AddUint32(&i, 1)

			select {
			case <-done:
				return
			default:
			}
		}
	}()

	return func() {
		t.Logf("wrote %d commands in writeMessagesUntilDone", atomic.LoadUint32(&i))
		done <- struct{}{}
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
			// fmt.Println(first)
			scanner, err := c.DoRead(first.ID, 1)
			if err != nil {
				panic(err)
			}
			expectLineMatchID(t, scanner, first.Body, first.ID)
			expectAllScanned(t, conf, scanner)

			scanner, err = c.DoRead(first.ID, 2)
			if err != nil {
				panic(err)
			}
			expectLineMatchID(t, scanner, first.Body, first.ID)
			expectLineID(t, scanner, first.ID+1)
			expectAllScanned(t, conf, scanner)

			scanner, err = c.DoRead(first.ID+1, 1)
			if err != nil {
				panic(err)
			}
			expectLineID(t, scanner, first.ID+1)
			expectAllScanned(t, conf, scanner)
		}
	}

}

func expectAllScanned(t testing.TB, conf *config.Config, scanner *client.Scanner) {
	prevmsg := scanner.Message()

	var firstMsg *protocol.Message
	var lastMsg *protocol.Message
	for scanner.Scan() {
		if firstMsg == nil {
			firstMsg = scanner.Message()
		}
		lastMsg = scanner.Message()
	}

	if firstMsg != nil {
		t.Logf("unexpected additional message after %v: %+v", firstMsg, prevmsg)
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
	ps := protocol.NewScanner(conf, f)
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
	t.SkipNow()
	configs := []*config.Config{
		config.DefaultConfig,
		&config.Config{
			ServerTimeout:           1000,
			ClientTimeout:           1000,
			GracefulShutdownTimeout: 1000,
			LogFileMode:             0644,
			MaxBatchSize:            1024 * 1024,
			PartitionSize:           1024 * 1024,
			IndexCursorSize:         10,
			MaxPartitions:           5,
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
		runTest(t, "graceful shutdown", testGracefulShutdown, conf)
	}
}

func getConfigs() map[string]*config.Config {
	m := map[string]*config.Config{
		"simple": &config.Config{
			ServerTimeout:           200,
			ClientTimeout:           200,
			GracefulShutdownTimeout: 200,
			LogFileMode:             0644,
			MaxBatchSize:            1024 * 1024,
			PartitionSize:           1024 * 1024,
			IndexCursorSize:         10,
			MaxPartitions:           5,
		},
	}

	if !testing.Short() {
		m["default"] = config.DefaultConfig
	}

	return m
}

func runTestConfs(t *testing.T, tf func(t *testing.T, c *config.Config)) {
	for label, conf := range getConfigs() {
		conf.Verbose = testing.Verbose()
		conf.LogFile = testhelper.TmpLog()

		t.Logf("config %s: %s", label, conf)
		tf(t, conf)
	}
}

func TestServerStartStop(t *testing.T) {
	runTestConfs(t, testServerStartStop)
}

func testServerStartStop(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	stopServer(t, srv)
}

func TestServerClientConnect(t *testing.T) {
	runTestConfs(t, testServerClientConnect)
}

func testServerClientConnect(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	c.Close()

	if err := waitForNoConnections(srv); err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestServerPing(t *testing.T) {
	runTestConfs(t, testServerPing)
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

func TestServerMsg(t *testing.T) {
	runTestConfs(t, testServerMsg)
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

func TestServerRead(t *testing.T) {
	runTestConfs(t, testServerRead)
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
	expectAllScanned(t, conf, scanner)
}

func TestServerReadNewWrite(t *testing.T) {
	runTestConfs(t, testServerReadNewWrite)
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

	expectNoLineRead(t, scanner)
}

func TestServerReadClose(t *testing.T) {
	runTestConfs(t, testServerReadClose)
}

func testServerReadClose(t *testing.T, conf *config.Config) {
	srv := newTestServer(conf)
	defer stopServer(t, srv)

	c := newTestClient(conf, srv)
	defer c.Close()

	msg := []byte("cool message")
	resp, err := c.Do(protocol.NewCommand(conf, protocol.CmdMessage, msg))
	testhelper.CheckError(err)
	expectRespOKID(t, resp, 1)

	cmds := createMessageCommands(conf, 1, 20)
	defer writeMessagesUntilDone(t, conf, c, cmds)()

	for n := 0; n < 10; n++ {
		readClient := newTestClient(conf, srv)

		_, err := readClient.DoRead(1, 10)
		testhelper.CheckError(err)
		readClient.Close()

		readClient = newTestClient(conf, srv)
		scanner, err := readClient.DoRead(1, 10)
		testhelper.CheckError(err)
		expectLineMatch(t, scanner, msg)

		for i := 0; i < n/2; i++ {
			scanner.Scan()
			testhelper.CheckError(scanner.Error())
			t.Logf("scanned %+v", scanner.Message())
		}

		readClient.Close()

		if _, err := readClient.Do(protocol.NewCommand(conf, protocol.CmdPing)); err == nil {
			t.Fatalf("command succeeded after close command")
		}

	}

}

func TestServerTail(t *testing.T) {
	runTestConfs(t, testServerTail)
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
	defer func() {
		conf.ReadFromTail = old
	}()
	scanner, err := c.DoRead(1, 1)
	testhelper.CheckError(err)

	expectLineMatch(t, scanner, msg)
}

func TestServerTailNewWrite(t *testing.T) {
	runTestConfs(t, testServerTailNewWrite)
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

	expectNoLineRead(t, scanner)
}

func TestServerTailFromTail(t *testing.T) {
	runTestConfs(t, testServerTailFromTail)
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

func TestServerSleep(t *testing.T) {
	runTestConfs(t, testServerSleep)
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

func TestServerInvalidRequests(t *testing.T) {
	runTestConfs(t, testServerInvalidRequests)
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

func TestServerWritePartition(t *testing.T) {
	runTestConfs(t, testServerWritePartition)
}

func testServerWritePartition(t *testing.T, conf *config.Config) {
	cmds := createMessageCommands(conf, 500, conf.PartitionSize)

	srv := newTestServer(conf)
	defer stopServer(t, srv)
	c := newTestClient(conf, srv)
	defer c.Close()

	for i := 0; i < conf.MaxPartitions; i++ {
		t.Logf("filling partition %d with %d messages", i, len(cmds))

		writeMessages(t, conf, c, cmds)

		part, err := ioutil.ReadFile(fmt.Sprintf("%s.%d", conf.LogFile, i))
		testhelper.CheckError(err)
		if len(part) > conf.PartitionSize {
			t.Fatalf("partition was larger than partition size: %d", conf.PartitionSize)
		}

		expectPartitionReads(t, conf, c)

		// XXX we don't account for the protocol envelope when calculating
		// message sizes, so we can't be sure if there will be a later
		// partition or not

		// _, err = ioutil.ReadFile(conf.LogFile + fmt.Sprintf(".%d", i+1))
		// if err == nil {
		// 	t.Fatalf("Expected no partition %d", i+1)
		// }
	}
}

func TestConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	runTestConfs(t, testConcurrentWrites)
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

type commandArgs struct {
	kind protocol.CmdType
	args []string
}

var someCommands []commandArgs = []commandArgs{
	{protocol.CmdPing, nil},
	{protocol.CmdHead, nil},
	{protocol.CmdClose, nil},
	{protocol.CmdStats, nil},
	{protocol.CmdMessage, []string{"cool message"}},
	// {protocol.CmdRead, []string{"1", "1"}},
	// {protocol.CmdTail, []string{"1", "1"}},
	{protocol.CmdSleep, []string{"75"}},
}

func randomCommand(conf *config.Config) *protocol.Command {
	raw := someCommands[rand.Intn(len(someCommands))]
	var args [][]byte
	for _, arg := range raw.args {
		args = append(args, []byte(arg))
	}
	return protocol.NewCommand(conf, raw.kind, args...)
}

func TestGracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	runTestConfs(t, testGracefulShutdown)
}

func testGracefulShutdown(t *testing.T, conf *config.Config) {
	for n := 0; n < 5; n++ {
		srv := newTestServer(conf)
		wg := sync.WaitGroup{}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				c := newTestClient(conf, srv)
				defer wg.Done()

				cmd := randomCommand(conf)
				c.Do(cmd)
			}()
		}

		if err := srv.Stop(); err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}

		if err := expectNoConnections(srv); err != nil {
			t.Fatalf("leftover connections after shutdown")
		}

		wg.Wait()
	}
}
