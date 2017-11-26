package logd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"runtime/debug"
	"testing"
	"time"
)

func testConfig(logger Logger) *Config {
	config := NewConfig()
	config.ServerTimeout = 500
	config.ClientTimeout = 500
	config.MaxChunkSize = 1024 * 10
	config.PartitionSize = 1024 * 1024 * 500
	config.LogFileMode = 0644
	config.IndexCursorSize = 1000

	// logger := newMemLogger()
	// logger.returnErr = loggerShouldErr

	config.Verbose = testing.Verbose()
	config.Logger = logger

	return config
}

func startQConfig(t testing.TB, config *Config) *eventQ {
	q := newEventQ(config)
	if err := q.start(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error starting queue: %+v", err)
	}
	return q
}

func startQ(t *testing.T, logger Logger) *eventQ {
	return startQConfig(t, testConfig(logger))
}

func stopQ(t testing.TB, q *eventQ) {
	if err := q.stop(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error stopping queue: %+v", err)
	}
}

func checkNoErrAndSuccess(t *testing.T, resp *Response, err error) {
	if err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error adding command to queue: %+v", err)
	}
	if resp == nil {
		t.Logf("%s", debug.Stack())
		t.Fatal("Expected response but got nil")
	}
	if resp.Status != RespOK {
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

func checkMessageReceived(t *testing.T, resp *Response, expectedID uint64, expectedMsg []byte) {
	// var ok bool

	msgb, err := readAllPending(resp.readerC)
	if err != nil {
		t.Fatalf("error reading pending data: %+v\n\n%s", err, debug.Stack())
	}

	// select {
	// case r, recvd := <-resp.readerC:
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

	fmt.Printf("recv: %q\n", msgb)
	msg, err := msgFromBytes(msgb)
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

func checkEOF(t *testing.T, resp *Response) {
	select {
	case r, recvd := <-resp.readerC:
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

func checkErrResp(t *testing.T, resp *Response) {
	if resp.Status != RespErr {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected error result but got %+v", resp.Status)
	}
}

func checkClientErrResp(t *testing.T, resp *Response) {
	if resp.Status != RespErrClient {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected client error result but got %+v", resp.Status)
	}
}

// use this to help debug deadlocks, more helpful probably to just use:
// kill -ABRT <pid>
func finishCommand(cmd *Command) {
	select {
	case <-cmd.done:
	case <-time.After(500 * time.Millisecond):
		panic("failed to finish command")
	}
}

func TestEventQStartStop(t *testing.T) {
	q := startQ(t, newMemLogger())
	stopQ(t, q)
}

func TestEventQAdd(t *testing.T) {
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(config, CmdPing))
	checkNoErrAndSuccess(t, resp, err)
}

func TestEventQWrite(t *testing.T) {
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(config, CmdMessage, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.ID)
	}
}

func TestEventQWriteErr(t *testing.T) {
	config := defaultTestConfig()
	memLogger := newMemLogger()
	memLogger.returnErr = true
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(config, CmdMessage, []byte("Hello, log!")))
	checkErrResp(t, resp)
}

func TestEventQWriteFilePartitions(t *testing.T) {
	config := testConfig(nil)
	config.IndexCursorSize = 10
	config.PartitionSize = 500
	config.LogFile = tmpLog()
	config, _, teardown := setupFileLoggerConfig(t, config)
	defer teardown()

	q := startQConfig(t, config)
	defer stopQ(t, q)

	msg := []byte(repeat("A", 50))
	for i := 0; i < 10; i++ {
		truncated := msg[:len(msg)-(10%(i+1))]
		resp, err := q.pushCommand(NewCommand(config, CmdMessage, truncated))
		checkNoErrAndSuccess(t, resp, err)
	}

	part1, _ := ioutil.ReadFile(config.LogFile + ".0")
	checkGoldenFile(t, "events.file_partition_write.0", part1, golden)
	part2, _ := ioutil.ReadFile(config.LogFile + ".1")
	checkGoldenFile(t, "events.file_partition_write.1", part2, golden)
	part3, _ := ioutil.ReadFile(config.LogFile + ".2")
	checkGoldenFile(t, "events.file_partition_write.2", part3, golden)
	_, err := ioutil.ReadFile(config.LogFile + ".3")
	if err == nil {
		t.Fatal("Expected no fourth partition")
	}

	index, _ := ioutil.ReadFile(config.LogFile + ".index")
	checkGoldenFile(t, "events.file_partition_write.index", index, golden)
}

func TestEventQWriteAfterRestart(t *testing.T) {
	config := testConfig(nil)
	config.IndexCursorSize = 1
	config.PartitionSize = 5000
	config.LogFile = tmpLog()
	config, _, teardown := setupFileLoggerConfig(t, config)
	defer teardown()

	q := startQConfig(t, config)
	// defer stopQ(t, q)

	for _, line := range someLines[:2] {
		resp, err := q.pushCommand(NewCommand(config, CmdMessage, line))
		checkNoErrAndSuccess(t, resp, err)
	}

	if err := CheckIndex(config); err != nil {
		t.Fatalf("bad index before restart: %+v", err)
	}

	stopQ(t, q)
	q = startQConfig(t, config)
	defer stopQ(t, q)

	for _, line := range someLines {
		resp, err := q.pushCommand(NewCommand(config, CmdMessage, line))
		checkNoErrAndSuccess(t, resp, err)
	}

	// logger.Flush()
	if err := CheckIndex(config); err != nil {
		t.Fatalf("bad index after restart: %+v", err)
	}

	part1, _ := ioutil.ReadFile(config.LogFile + ".0")
	checkGoldenFile(t, "events.write_after_restart.0", part1, golden)
	part2, _ := ioutil.ReadFile(config.LogFile + ".1")
	checkGoldenFile(t, "events.write_after_restart.1", part2, golden)
	_, err := ioutil.ReadFile(config.LogFile + ".2")
	if err == nil {
		t.Fatal("Expected no third partition")
	}

	index, _ := ioutil.ReadFile(config.LogFile + ".index")
	checkGoldenFile(t, "events.write_after_restart.index", index, golden)
}

// TODO this is maybe too tightly coupled to the prev test
// It's copied from the golden file the events.file_partition_write.[0-3]
func TestEventQReadFilePartitions(t *testing.T) {
	t.SkipNow()
	config := testConfig(nil)
	config.IndexCursorSize = 10
	config.PartitionSize = 500
	config.LogFile = path.Join("testdata", "q.read_file_test_log")
	logger := newFileLogger(config)
	config.Logger = logger

	q := startQConfig(t, config)
	defer stopQ(t, q)

	msg := []byte(repeat("A", 50))
	for i := 0; i < 10; i++ {
		truncated := msg[:len(msg)-(10%(i+1))]
		id := i + 1
		t.Logf("checking id %d", id)

		idStr := fmt.Sprintf("%d", id)
		cmd := NewCommand(config, CmdRead, []byte(idStr), []byte("1"))
		resp, err := q.pushCommand(cmd)
		cmd.signalReady()

		checkNoErrAndSuccess(t, resp, err)
		checkMessageReceived(t, resp, uint64(id), truncated)
		checkEOF(t, resp)
	}
}

func TestEventQHead(t *testing.T) {
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(config, CmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 0 {
		t.Fatalf("Expected response with id 0 but got %d", resp.ID)
	}

	resp, err = q.pushCommand(NewCommand(config, CmdMessage, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.ID)
	}

	resp, err = q.pushCommand(NewCommand(config, CmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.ID != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.ID)
	}
}

func TestEventQHeadErr(t *testing.T) {
	memLogger := newMemLogger()
	config := testConfig(memLogger)
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	memLogger.headReturnErr = errors.New("cool error")
	resp, _ := q.pushCommand(NewCommand(config, CmdHead))
	checkErrResp(t, resp)

	anotherQ := newEventQ(config)
	if err := anotherQ.start(); err == nil {
		t.Fatalf("expected error starting queue but got none")
	}
}

func TestEventQReplicate(t *testing.T) {
	t.SkipNow()
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.pushCommand(NewCommand(config, CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	cmd := NewCommand(config, CmdReplicate, []byte("1"))
	tailResp, err := q.pushCommand(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	resp, err = q.pushCommand(NewCommand(config, CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 2, expectedMsg)

	closeCmd := newCloseCommand(config, cmd.respC)
	closeResp, err := q.pushCommand(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)
}

func TestEventQRead(t *testing.T) {
	t.SkipNow()
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.pushCommand(NewCommand(config, CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	cmd := NewCommand(config, CmdRead, []byte("1"), []byte("1"))
	tailResp, err := q.pushCommand(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)
	checkEOF(t, tailResp)

	cmd = NewCommand(config, CmdRead, []byte("1"), []byte("0"))
	tailResp, err = q.pushCommand(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	resp, err = q.pushCommand(NewCommand(config, CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 2, expectedMsg)

	closeCmd := newCloseCommand(config, cmd.respC)
	closeResp, err := q.pushCommand(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)

}

func TestEventQReadFromEmpty(t *testing.T) {
	t.SkipNow()
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	tailResp, err := q.pushCommand(NewCommand(config, CmdRead, []byte("1"), []byte("0")))
	checkNoErrAndSuccess(t, tailResp, err)

	resp, err := q.pushCommand(NewCommand(config, CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 1, expectedMsg)
}

func TestEventQReadErr(t *testing.T) {
	config := defaultTestConfig()
	memLogger := newMemLogger()
	memLogger.returnErr = true
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(config, CmdRead))
	checkClientErrResp(t, resp)
}

func TestEventQReadClose(t *testing.T) {
	t.SkipNow()
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.pushCommand(NewCommand(config, CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	readCmd := NewCommand(config, CmdRead, []byte("1"), []byte("0"))
	tailResp, err := q.pushCommand(readCmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	closeCmd := newCloseCommand(config, readCmd.respC)
	closeResp, err := q.pushCommand(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)

	resp, err = q.pushCommand(NewCommand(config, CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	if len(q.subscriptions) > 0 {
		t.Fatalf("Expected subscriptions to be empty but had %d subscribers", len(q.subscriptions))
	}
}

func TestEventQReadInvalidParams(t *testing.T) {
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(config, CmdRead, []byte("asdf"), []byte("1")))
	checkClientErrResp(t, resp)

	resp, _ = q.pushCommand(NewCommand(config, CmdRead, []byte("1"), []byte("asdf")))
	checkClientErrResp(t, resp)

	resp, _ = q.pushCommand(NewCommand(config, CmdRead, []byte("1")))
	checkClientErrResp(t, resp)
}

func TestEventQUnknownCommand(t *testing.T) {
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(config, 100))
	checkErrResp(t, resp)
}

func TestEventQSleep(t *testing.T) {
	config := defaultTestConfig()
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(config, CmdSleep, []byte("0")))
	checkNoErrAndSuccess(t, resp, err)

	resp, err = q.pushCommand(NewCommand(config, CmdSleep, []byte("1")))
	checkNoErrAndSuccess(t, resp, err)

	done := make(chan struct{})
	sleepCmd := NewCommand(config, CmdSleep, []byte("100"))

	go func() {
		resp, err = q.pushCommand(sleepCmd)
		checkNoErrAndSuccess(t, resp, err)
		done <- struct{}{}
	}()

	sleepCmd.cancelSleep()

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("Failed to cancel sleep")
	}
}
