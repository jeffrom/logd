// +build !race

package logd

// testing an internal command that should only be used for testing

import "testing"

func TestEventQShutdown(t *testing.T) {
	q := startQ(t, newMemLogger())
	resp, err := q.pushCommand(NewCommand(CmdShutdown))
	checkNoErrAndSuccess(t, resp, err)

	defer func() {
		if recover() == nil {
			t.Fatal("Expected panic from writing to closed channel but got none")
		}
	}()

	q.pushCommand(NewCommand(CmdPing))
}
