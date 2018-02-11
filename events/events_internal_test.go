// +build !race

package events

// testing an internal command that should only be used for testing

import (
	"testing"

	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestEventQShutdown(t *testing.T) {
	t.SkipNow()
	config := testhelper.DefaultTestConfig(testing.Verbose())
	q := startQ(t, logger.NewMemLogger())
	resp, err := q.PushCommand(protocol.NewCommand(config, protocol.CmdShutdown))
	checkNoErrAndSuccess(t, resp, err)

	defer func() {
		if recover() == nil {
			t.Fatal("Expected panic from writing to closed channel but got none")
		}
	}()

	q.PushCommand(protocol.NewCommand(config, protocol.CmdPing))
}
