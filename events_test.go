package logd

import (
	"testing"
)

func testConfig() *Config {
	config := NewConfig()

	config.Verbose = true
	config.Logger = newMemLogger()

	return config
}

func startQ(t *testing.T) *eventQ {
	q := newEventQ(testConfig())
	if err := q.start(); err != nil {
		t.Fatalf("error starting queue: %v", err)
	}
	return q
}

func stopQ(t *testing.T, q *eventQ) {
	if err := q.stop(); err != nil {
		t.Fatalf("error stopping queue: %v", err)
	}
}

func TestEventQStartStop(t *testing.T) {
	q := startQ(t)
	stopQ(t, q)
}

func TestEventQAdd(t *testing.T) {
	q := startQ(t)
	defer stopQ(t, q)

	resp, err := q.add(newCommand(cmdPing))
	if err != nil {
		t.Fatalf("error adding command to queue: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected response but got nil")
	}
	if resp.status != respOK {
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}
}

func TestEventQShutdown(t *testing.T) {
	q := startQ(t)
	resp, err := q.add(newCommand(cmdShutdown))
	if err != nil {
		t.Fatalf("error adding command to queue: %v", err)
	}
	if resp.status != respOK {
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Expected panic from writing to closed channel but got none")
		}
	}()

	q.add(newCommand(cmdPing))
}
func TestEventQLog(t *testing.T) {
	q := startQ(t)
	defer stopQ(t, q)

	resp, err := q.add(newCommand(cmdMsg, []byte("Hello, log!")))
	if err != nil {
		t.Fatalf("error adding command to queue: %v", err)
	}

	if resp.status != respOK {
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}
}

func TestEventQHead(t *testing.T) {
	q := startQ(t)
	defer stopQ(t, q)

	resp, err := q.add(newCommand(cmdHead))
	if err != nil {
		t.Fatalf("error adding HEAD command to queue: %v", err)
	}
	if resp.status != respOK {
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}
	if resp.id != 0 {
		t.Fatalf("Expected response with id 0 but got %d", resp.id)
	}

	resp, err = q.add(newCommand(cmdMsg, []byte("Hello, log!")))
	if err != nil {
		t.Fatalf("error adding MSG command to queue: %v", err)
	}

	if resp.status != respOK {
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}

	resp, err = q.add(newCommand(cmdHead))
	if err != nil {
		t.Fatalf("error adding HEAD command to queue: %v", err)
	}
	if resp.status != respOK {
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}
	if resp.id != 1 {
		t.Fatalf("Expected response with id 1 but got %d", resp.id)
	}
}
