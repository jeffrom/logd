package logd

import (
	"bytes"
	"runtime/debug"
	"testing"
	"time"
)

func testConfig(loggerShouldErr bool) *Config {
	config := NewConfig()

	logger := newMemLogger()
	logger.returnErr = loggerShouldErr

	config.Verbose = true
	config.Logger = logger

	return config
}

func startQ(t *testing.T, loggerShouldErr bool) *eventQ {
	q := newEventQ(testConfig(loggerShouldErr))
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

func checkNoErrAndSuccess(t *testing.T, resp *response, err error) {
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

func checkMessageReceived(t *testing.T, resp *response, expectedID uint64, expectedMsg []byte) {
	msg, ok := <-resp.msgC
	if !ok {
		t.Logf("%s", debug.Stack())
		t.Fatal("Expected to read message but got none")
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

func checkErrResp(t *testing.T, resp *response) {
	if resp.status != respErr {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Expected error result but got %v", resp.status)
	}
}

// use this to help debug deadlocks, more helpful probably to just use:
// kill -ABRT <pid>
func finishCommand(cmd *command) {
	select {
	case <-cmd.done:
	case <-time.After(500 * time.Millisecond):
		panic("failed to finish command")
	}
}

func TestEventQStartStop(t *testing.T) {
	q := startQ(t, false)
	stopQ(t, q)
}

func TestEventQAdd(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	resp, err := q.add(newCommand(cmdPing))
	checkNoErrAndSuccess(t, resp, err)
}

func TestEventQLog(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	resp, err := q.add(newCommand(cmdMsg, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}
}

func TestEventQLogErr(t *testing.T) {
	q := startQ(t, true)
	defer stopQ(t, q)

	resp, _ := q.add(newCommand(cmdMsg, []byte("Hello, log!")))
	checkErrResp(t, resp)
}

func TestEventQHead(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	resp, err := q.add(newCommand(cmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 0 {
		t.Fatalf("Expected response with id 0 but got %d", resp.id)
	}

	resp, err = q.add(newCommand(cmdMsg, []byte("Hello, log!")))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}

	resp, err = q.add(newCommand(cmdHead))
	checkNoErrAndSuccess(t, resp, err)
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}
}

func TestEventQHeadErr(t *testing.T) {
	q := startQ(t, true)
	defer stopQ(t, q)

	resp, _ := q.add(newCommand(cmdHead))
	checkErrResp(t, resp)
}

func TestEventQRead(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.add(newCommand(cmdMsg, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	cmd := newCommand(cmdRead, []byte("1"), []byte("1"))
	tailResp, err := q.add(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	cmd = newCommand(cmdRead, []byte("1"), []byte("0"))
	tailResp, err = q.add(cmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	resp, err = q.add(newCommand(cmdMsg, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 2, expectedMsg)

	closeCmd := newCloseCommand(cmd.respC)
	closeResp, err := q.add(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)
}

func TestEventQReadFromEmpty(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	tailResp, err := q.add(newCommand(cmdRead, []byte("1"), []byte("0")))
	checkNoErrAndSuccess(t, tailResp, err)

	resp, err := q.add(newCommand(cmdMsg, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	checkMessageReceived(t, tailResp, 1, expectedMsg)
}

func TestEventQReadErr(t *testing.T) {
	q := startQ(t, true)
	defer stopQ(t, q)

	resp, _ := q.add(newCommand(cmdRead))
	checkErrResp(t, resp)
}

func TestEventQReadClose(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	expectedMsg := []byte("Hello, log!")

	resp, err := q.add(newCommand(cmdMsg, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	readCmd := newCommand(cmdRead, []byte("1"), []byte("0"))
	tailResp, err := q.add(readCmd)
	checkNoErrAndSuccess(t, tailResp, err)
	checkMessageReceived(t, tailResp, 1, expectedMsg)

	closeCmd := newCloseCommand(readCmd.respC)
	closeResp, err := q.add(closeCmd)
	checkNoErrAndSuccess(t, closeResp, err)

	resp, err = q.add(newCommand(cmdMsg, expectedMsg))
	checkNoErrAndSuccess(t, resp, err)

	if len(q.subscriptions) > 0 {
		t.Fatalf("Expected subscriptions to be empty but had %d subscribers", len(q.subscriptions))
	}
}

func TestEventQReadInvalidParams(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	resp, _ := q.add(newCommand(cmdRead, []byte("asdf"), []byte("1")))
	checkErrResp(t, resp)

	resp, _ = q.add(newCommand(cmdRead, []byte("1"), []byte("asdf")))
	checkErrResp(t, resp)

	resp, _ = q.add(newCommand(cmdRead, []byte("1")))
	checkErrResp(t, resp)
}

func TestEventQUnknownCommand(t *testing.T) {
	q := startQ(t, false)
	defer stopQ(t, q)

	resp, _ := q.add(newCommand(100))
	checkErrResp(t, resp)
}
