package events

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"runtime/debug"
	"testing"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

func startQConfig(t testing.TB, l logger.Logger, conf *config.Config) *EventQ {
	q := NewEventQ(conf)
	t.Logf("starting event queue with config: %+v", conf)
	q.log = l
	if err := q.Start(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error starting queue: %+v", err)
	}
	return q
}

func startQ(t *testing.T, l logger.Logger) *EventQ {
	return startQConfig(t, l, testhelper.TestConfig(testing.Verbose()))
}

func stopQ(t testing.TB, q *EventQ) {
	if err := q.Stop(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error stopping queue: %+v", err)
	}
}

func checkNoErrAndSuccess(t *testing.T, resp *protocol.Response, err error) {
	if err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error adding command to queue: %+v", err)
	}
	if resp == nil {
		t.Logf("%s", debug.Stack())
		t.Fatal("Expected response but got nil")
	}
	if resp.Status != protocol.RespOK {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected OK response but got %s", resp.Status.String())
	}
}

func readAllPending(readerC chan io.Reader) ([]byte, error) {
	numRead := 0
	var read int64
	buf := bytes.Buffer{}
	var err error
Loop:
	for {
		select {
		case r := <-readerC:
			var n int64
			n, err = buf.ReadFrom(r)
			numRead++
			read += n

			if closer, ok := r.(io.Closer); ok {
				if cerr := closer.Close(); cerr != nil {
					return nil, cerr
				}
			}
			if err != nil {
				return nil, err
			}
		default:
			break Loop
		}
	}

	return buf.Bytes(), err
}

func checkMessageReceived(t *testing.T, resp *protocol.Response, expectedID uint64, expectedMsg []byte) {
	// var ok bool

	msgb, err := readAllPending(resp.ReaderC)
	if err != nil {
		t.Fatalf("error reading pending data: %+v\n\n%s", err, debug.Stack())
	}

	// select {
	// case r, recvd := <-resp.ReaderC:
	// 	b, err := ioutil.ReadAll(r)
	// 	if err != nil {
	// 		t.Fatalf("error reading response: %+v", err)
	// 	}
	// 	msgb = b
	// 	ok = recvd
	// case <-time.After(time.Millisecond * 100):
	// 	t.Logf("%s", debug.Stack())
	// 	t.Fatalf("timed out waiting for message %d on channel", expectedID)
	// }

	// if !ok {
	// 	t.Logf("%s", debug.Stack())
	// 	t.Fatal("Expected to read message but got none")
	// }

	// fmt.Printf("recv: %q\n", msgb)
	msg, err := protocol.MsgFromBytes(msgb)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if msg.ID != expectedID {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected message with id %d but got %d", expectedID, msg.ID)
	}
	if !bytes.Equal(msg.Body, expectedMsg) {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected message %q but got %q", expectedMsg, msg.Body)
	}
}

func checkEOF(t *testing.T, resp *protocol.Response) {
	select {
	case r, recvd := <-resp.ReaderC:
		b, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatalf("error reading response: %+v", err)
		}

		if !recvd {
			t.Fatal("Expected +EOF but received nothing")
		}
		if !bytes.Equal(b, []byte("+EOF\r\n")) {
			t.Fatalf("Expected +EOF but got %q", b)
		}
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timed out waiting for +EOF on channel")
	}
}

func checkErrResp(t *testing.T, resp *protocol.Response) {
	if resp.Status != protocol.RespErr {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected error result but got %+v", resp.Status)
	}
}

func checkClientErrResp(t *testing.T, resp *protocol.Response) {
	if resp.Status != protocol.RespErrClient {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected client error result but got %+v", resp.Status)
	}
}

func TestEventQStartStop(t *testing.T) {
	q := startQ(t, logger.NewMemLogger())
	stopQ(t, q)
}

func TestEventQAdd(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdPing))
	checkNoErrAndSuccess(t, resp, err)
}

func TestEventQWrite(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.ID)
	}
}

func TestEventQWriteErr(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	memLogger := logger.NewMemLogger()
	memLogger.ReturnErr = true
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	resp, _ := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, []byte("Hello, log!")))
	checkErrResp(t, resp)
}

func TestEventQWriteFilePartitions(t *testing.T) {
	config := testhelper.TestConfig(testing.Verbose())
	config.IndexCursorSize = 10
	config.PartitionSize = 500
	config.LogFile = testhelper.TmpLog()
	config, _, teardown := logger.SetupTestFileLoggerConfig(config, testing.Verbose())
	defer teardown()

	q := startQConfig(t, logger.NewFileLogger(config), config)
	defer stopQ(t, q)

	msg := []byte(testhelper.Repeat("A", 50))
	for i := 0; i < 10; i++ {
		truncated := msg[:len(msg)-(10%(i+1))]
		resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, truncated))
		checkNoErrAndSuccess(t, resp, err)
	}

	part1, _ := ioutil.ReadFile(config.LogFile + ".0")
	testhelper.CheckGoldenFile("events.file_partition_write.0", part1, testhelper.Golden)
	part2, _ := ioutil.ReadFile(config.LogFile + ".1")
	testhelper.CheckGoldenFile("events.file_partition_write.1", part2, testhelper.Golden)
	part3, _ := ioutil.ReadFile(config.LogFile + ".2")
	testhelper.CheckGoldenFile("events.file_partition_write.2", part3, testhelper.Golden)
	_, err := ioutil.ReadFile(config.LogFile + ".3")
	if err == nil {
		t.Fatal("Expected no fourth partition")
	}

	index, _ := ioutil.ReadFile(config.LogFile + ".index")
	testhelper.CheckGoldenFile("events.file_partition_write.index", index, testhelper.Golden)
}

func TestEventQWriteAfterRestart(t *testing.T) {
	config := testhelper.TestConfig(testing.Verbose())
	config.IndexCursorSize = 1
	config.PartitionSize = 5000
	config.LogFile = testhelper.TmpLog()
	config, _, teardown := logger.SetupTestFileLoggerConfig(config, testing.Verbose())
	defer teardown()

	q := startQConfig(t, logger.NewFileLogger(config), config)
	// defer stopQ(t, q)

	for _, line := range testhelper.SomeLines[:2] {
		resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, line))
		checkNoErrAndSuccess(t, resp, err)
	}

	if err := logger.CheckIndex(config); err != nil {
		t.Fatalf("bad index before restart: %+v", err)
	}

	stopQ(t, q)
	q = startQConfig(t, logger.NewFileLogger(config), config)
	defer stopQ(t, q)

	for _, line := range testhelper.SomeLines {
		resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, line))
		checkNoErrAndSuccess(t, resp, err)
	}

	// logger.Flush()
	if err := logger.CheckIndex(config); err != nil {
		t.Fatalf("bad index after restart: %+v", err)
	}

	part1, _ := ioutil.ReadFile(config.LogFile + ".0")
	testhelper.CheckGoldenFile("events.write_after_restart.0", part1, testhelper.Golden)
	part2, _ := ioutil.ReadFile(config.LogFile + ".1")
	testhelper.CheckGoldenFile("events.write_after_restart.1", part2, testhelper.Golden)
	_, err := ioutil.ReadFile(config.LogFile + ".2")
	if err == nil {
		t.Fatal("Expected no third partition")
	}

	index, _ := ioutil.ReadFile(config.LogFile + ".index")
	testhelper.CheckGoldenFile("events.write_after_restart.index", index, testhelper.Golden)
}

// TODO this is maybe too tightly coupled to the prev test
// It's copied from the testhelper.Golden file the events.file_partition_write.[0-3]
func TestEventQReadFilePartitions(t *testing.T) {
	t.SkipNow()
	config := testhelper.TestConfig(testing.Verbose())
	config.IndexCursorSize = 10
	config.PartitionSize = 500
	config.LogFile = path.Join("testdata", "q.read_file_test_log")

	q := startQConfig(t, logger.NewFileLogger(config), config)
	defer stopQ(t, q)

	msg := []byte(testhelper.Repeat("A", 50))
	for i := 0; i < 10; i++ {
		truncated := msg[:len(msg)-(10%(i+1))]
		id := i + 1
		t.Logf("checking id %d", id)

		idStr := fmt.Sprintf("%d", id)
		cmd := protocol.NewCommand(config, protocol.CmdRead, []byte(idStr), []byte("1"))
		resp, err := q.PushCommand(context.Background(), cmd)
		cmd.SignalReady()

		checkNoErrAndSuccess(t, resp, err)
		checkMessageReceived(t, resp, uint64(id), truncated)
		checkEOF(t, resp)
	}
}

func TestEventQHead(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 0 {
		t.Fatalf("Expected response with id 0 but got %d", resp.ID)
	}

	resp, err = q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.ID)
	}

	resp, err = q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.ID)
	}
}

func TestEventQHeadErr(t *testing.T) {
	memLogger := logger.NewMemLogger()
	config := testhelper.TestConfig(testing.Verbose())
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	memLogger.HeadReturnErr = errors.New("cool error")
	resp, _ := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdHead))
	checkErrResp(t, resp)

	anotherQ := NewEventQ(config)
	anotherQ.log = memLogger
	if err := anotherQ.Start(); err == nil {
		t.Fatalf("expected error starting queue but got none")
	}
}

func TestEventQRead(t *testing.T) {
	t.SkipNow()
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	cmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("1"))
	tailResp, err := q.PushCommand(context.Background(), cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)
	checkEOF(t, tailResp)

	cmd = protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("0"))
	tailResp, err = q.PushCommand(context.Background(), cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	resp, err = q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 2, expectedMsg)

	closeCmd := protocol.NewCloseCommand(config, cmd.RespC)
	closeResp, err := q.PushCommand(context.Background(), closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)

}

func TestEventQReadFromEmpty(t *testing.T) {
	t.SkipNow()
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	tailResp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("0")))
	checkNoErrAndSuccess(t, tailResp, err)

	resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 1, expectedMsg)
}

func TestEventQReadErr(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	memLogger := logger.NewMemLogger()
	memLogger.ReturnErr = true
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	resp, _ := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdRead))
	checkClientErrResp(t, resp)
}

func TestEventQReadClose(t *testing.T) {
	t.SkipNow()
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	readCmd := protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("0"))
	tailResp, err := q.PushCommand(context.Background(), readCmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	closeCmd := protocol.NewCloseCommand(config, readCmd.RespC)
	closeResp, err := q.PushCommand(context.Background(), closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)

	resp, err = q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	if len(q.subscriptions) > 0 {
		t.Fatalf("Expected subscriptions to be empty but had %d subscribers", len(q.subscriptions))
	}
}

func TestEventQReadInvalidParams(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	resp, _ := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdRead, []byte("asdf"), []byte("1")))
	checkClientErrResp(t, resp)

	resp, _ = q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdRead, []byte("1"), []byte("asdf")))
	checkClientErrResp(t, resp)

	resp, _ = q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdRead, []byte("1")))
	checkClientErrResp(t, resp)
}

func TestEventQUnknownCommand(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	resp, _ := q.PushCommand(context.Background(), protocol.NewCommand(config, 100))
	checkErrResp(t, resp)
}

func TestEventQSleep(t *testing.T) {
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	defer stopQ(t, q)

	resp, err := q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdSleep, []byte("0")))
	checkNoErrAndSuccess(t, resp, err)

	resp, err = q.PushCommand(context.Background(), protocol.NewCommand(config, protocol.CmdSleep, []byte("1")))
	checkNoErrAndSuccess(t, resp, err)

	done := make(chan struct{})
	sleepCmd := protocol.NewCommand(config, protocol.CmdSleep, []byte("100"))

	go func() {
		resp, err = q.PushCommand(context.Background(), sleepCmd)
		checkNoErrAndSuccess(t, resp, err)
		done <- struct{}{}
	}()

	sleepCmd.CancelSleep()

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("Failed to cancel sleep")
	}
}
