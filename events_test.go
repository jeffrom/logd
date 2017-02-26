package logd

import (
	"testing"
)

func testConfig() *Config {
	config := NewConfig()

	config.Verbose = true

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
		t.Fatalf("error adding command: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected response but got nil")
	}
	if resp.status != respOK {
		t.Fatalf("Expected OK response but got %s", resp.status.String())
	}
}
