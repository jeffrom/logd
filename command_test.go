package logd

import "testing"

func TestUnknownCommand(t *testing.T) {
	// remove this if it becomes a problem. this test is dumb
	unknownCmd := newCommand(100)
	s := unknownCmd.String()
	if s != "<unknown_command 'd'>/0" {
		t.Fatalf("improperly formatted command string: %s", s)
	}
}
