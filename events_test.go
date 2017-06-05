package logd

import (
	"bytes"
	"errors"
	"runtime/debug"
	"testing"
	"time"
)

func testConfig(logger Logger) *Config {
	config := NewConfig()

	// logger := newMemLogger()
	// logger.returnErr = loggerShouldErr

	config.Verbose = true
	config.Logger = logger

	return config
}

func startQ(t *testing.T, logger Logger) *eventQ {
	q := newEventQ(testConfig(logger))
	if err := q.start(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error starting queue: %v", err)
	}
	return q
}

func stopQ(t testing.TB, q *eventQ) {
	if err := q.stop(); err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error stopping queue: %v", err)
	}
}

func checkNoErrAndSuccess(t *testing.T, resp *Response, err error) {
	if err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("error adding command to queue: %v", err)
	}
	if resp == nil {
		t.Logf("%s", debug.Stack())
		t.Fatal("Expected response but got nil")
	}
	if resp.status != respOK {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}
}

func checkMessageReceived(t *testing.T, resp *Response, expectedID uint64, expectedMsg []byte) {
	msgb, ok := <-resp.msgC
	if !ok {
		t.Logf("%s", debug.Stack())
		t.Fatal("Expected to read message but got none")
	}
	msg, err := fromBytes(msgb)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if msg.id != expectedID {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected message with id %d but got %d", expectedID, msg.id)
	}
	if !bytes.Equal(msg.body, expectedMsg) {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected message %q but got %q", expectedMsg, msg.body)
	}
}

func checkErrResp(t *testing.T, resp *Response) {
	if resp.status != respErr {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected error result but got %v", resp.status)
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
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(CmdPing))
	checkNoErrAndSuccess(t, resp, err)
}

func TestEventQLog(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(CmdMessage, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}
}

func TestEventQLogErr(t *testing.T) {
	memLogger := newMemLogger()
	memLogger.returnErr = true
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(CmdMessage, []byte("Hello, log!")))
	checkErrResp(t, resp)
}

func TestEventQHead(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(CmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 0 {
		t.Fatalf("Expected response with id 0 but got %d", resp.id)
	}

	resp, err = q.pushCommand(NewCommand(CmdMessage, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}

	resp, err = q.pushCommand(NewCommand(CmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}
}

func TestEventQHeadErr(t *testing.T) {
	memLogger := newMemLogger()
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	memLogger.headReturnErr = errors.New("cool error")
	resp, _ := q.pushCommand(NewCommand(CmdHead))
	checkErrResp(t, resp)

	anotherQ := newEventQ(testConfig(memLogger))
	if err := anotherQ.start(); err == nil {
		t.Fatalf("expected error starting queue but got none")
	}
}

func TestEventQReplicate(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.pushCommand(NewCommand(CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	cmd := NewCommand(CmdReplicate, []byte("1"))
	tailResp, err := q.pushCommand(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	resp, err = q.pushCommand(NewCommand(CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 2, expectedMsg)

	closeCmd := newCloseCommand(cmd.respC)
	closeResp, err := q.pushCommand(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)
}

func TestEventQRead(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.pushCommand(NewCommand(CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	cmd := NewCommand(CmdRead, []byte("1"), []byte("1"))
	tailResp, err := q.pushCommand(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	cmd = NewCommand(CmdRead, []byte("1"), []byte("0"))
	tailResp, err = q.pushCommand(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	resp, err = q.pushCommand(NewCommand(CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 2, expectedMsg)

	closeCmd := newCloseCommand(cmd.respC)
	closeResp, err := q.pushCommand(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)
}

func TestEventQReadFromEmpty(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	tailResp, err := q.pushCommand(NewCommand(CmdRead, []byte("1"), []byte("0")))
	checkNoErrAndSuccess(t, tailResp, err)

	resp, err := q.pushCommand(NewCommand(CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 1, expectedMsg)
}

func TestEventQReadErr(t *testing.T) {
	memLogger := newMemLogger()
	memLogger.returnErr = true
	q := startQ(t, memLogger)
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(CmdRead))
	checkErrResp(t, resp)
}

func TestEventQReadClose(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.pushCommand(NewCommand(CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	readCmd := NewCommand(CmdRead, []byte("1"), []byte("0"))
	tailResp, err := q.pushCommand(readCmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	closeCmd := newCloseCommand(readCmd.respC)
	closeResp, err := q.pushCommand(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)

	resp, err = q.pushCommand(NewCommand(CmdMessage, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	if len(q.subscriptions) > 0 {
		t.Fatalf("Expected subscriptions to be empty but had %d subscribers", len(q.subscriptions))
	}
}

func TestEventQReadInvalidParams(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(CmdRead, []byte("asdf"), []byte("1")))
	checkErrResp(t, resp)

	resp, _ = q.pushCommand(NewCommand(CmdRead, []byte("1"), []byte("asdf")))
	checkErrResp(t, resp)

	resp, _ = q.pushCommand(NewCommand(CmdRead, []byte("1")))
	checkErrResp(t, resp)
}

func TestEventQUnknownCommand(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, _ := q.pushCommand(NewCommand(100))
	checkErrResp(t, resp)
}

func TestEventQSleep(t *testing.T) {
	q := startQ(t, newMemLogger())
	defer stopQ(t, q)

	resp, err := q.pushCommand(NewCommand(CmdSleep, []byte("0")))
	checkNoErrAndSuccess(t, resp, err)

	resp, err = q.pushCommand(NewCommand(CmdSleep, []byte("1")))
	checkNoErrAndSuccess(t, resp, err)

	done := make(chan struct{})
	sleepCmd := NewCommand(CmdSleep, []byte("100"))

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
