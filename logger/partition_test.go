package logger

import (
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestPartitionAdd(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	p := newFilePartitions(conf)
	p.add(0)
}
