package testhelper

import (
	"github.com/jeffrom/logd/config"
)

func TestConfig(verbose bool) *config.Config {
	conf := config.NewConfig()
	conf.ServerTimeout = 1000
	conf.ClientTimeout = 1000
	conf.GracefulShutdownTimeout = 1000
	conf.MaxChunkSize = 1024 * 10
	conf.PartitionSize = 1024 * 1024 * 500
	conf.LogFile = TmpLog()
	conf.LogFileMode = 0644
	conf.IndexCursorSize = 1000

	conf.Verbose = verbose

	return conf
}
