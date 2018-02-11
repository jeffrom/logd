package logger

import (
	"log"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func SetupTestFileLoggerConfig(conf *config.Config, verbose bool) (*config.Config, Logger, func()) {
	conf.Verbose = verbose
	var l Logger
	l = NewFileLogger(conf)

	if err := l.(LogManager).Setup(); err != nil {
		log.Fatalf("error setting up: %+v", err)
	}

	return conf, l, func() {
		// dir := path.Dir(conf.LogFile)
		// t.Logf("Deleting %s", dir)
		// if strings.Contains(dir, internal.LogDirPrefix) {
		// 	os.RemoveAll(dir)
		// }

		if err := l.(LogManager).Shutdown(); err != nil {
			log.Fatalf("error shutting down: %+v", err)
		}

	}
}

func SetupTestFileLogger(verbose bool) (*config.Config, Logger, func()) {
	conf := testhelper.TestConfig(verbose)
	conf.IndexCursorSize = 10
	conf.PartitionSize = 2048
	conf.LogFile = testhelper.TmpLog()

	// t.Logf("test logs at: %s", config.LogFile)
	return SetupTestFileLoggerConfig(conf, verbose)
}
