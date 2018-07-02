package events

import (
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestShutdown(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	q := NewEventQ(conf)
	for i := 0; i < 100; i++ {
		doStartQ(t, q)
		doShutdownQ(t, q)
	}
}
