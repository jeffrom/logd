package testhelper

import (
	"github.com/jeffrom/logd/config"
)

func TestConfig(verbose bool) *config.Config {
	conf := config.NewConfig()
	conf.ServerTimeout = 500
	conf.ClientTimeout = 500
	conf.MaxChunkSize = 1024 * 10
	conf.PartitionSize = 1024 * 1024 * 500
	conf.LogFile = TmpLog()
	conf.LogFileMode = 0644
	conf.IndexCursorSize = 1000

	conf.Verbose = verbose

	return conf
}
