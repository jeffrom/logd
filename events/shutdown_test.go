package events

import (
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestShutdown(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	q := NewHandlers(conf)
	for i := 0; i < 100; i++ {
		doStartHandler(t, q)
		doShutdownHandler(t, q)
	}
}
