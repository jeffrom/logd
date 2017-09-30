package main

import (
	"os"
	"sort"

	"gopkg.in/urfave/cli.v1"
	"gopkg.in/urfave/cli.v1/altsrc"

	"github.com/jeffrom/logd"
)

func runApp(args []string) {
	config := &logd.Config{}
	*config = *logd.DefaultConfig

	var check bool

	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "Print only the version",
	}

	app := cli.NewApp()
	app.Name = "logd"
	app.Usage = "networked logging server"
	app.Version = "0.0.1"
	app.EnableBashCompletion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "config, c",
			Usage:  "Load configuration from `FILE`",
			Value:  "logd_conf.yml",
			EnvVar: "LOGD_CONFIG",
		},
		altsrc.NewBoolFlag(cli.BoolFlag{
			Name:        "verbose, v",
			Usage:       "print debug output",
			EnvVar:      "LOGD_VERBOSE",
			Destination: &config.Verbose,
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:        "host",
			Usage:       "A `HOST:PORT` combination to connect to",
			EnvVar:      "LOGD_HOST",
			Value:       "127.0.0.1:1774",
			Destination: &config.Hostport,
		}),
		altsrc.NewUintFlag(cli.UintFlag{
			Name:        "timeout",
			Usage:       "Time, in milliseconds, to wait for a response to be acknowledged",
			EnvVar:      "LOGD_TIMEOUT",
			Value:       500,
			Destination: &config.ServerTimeout,
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:        "log_file",
			Usage:       "Log file name",
			EnvVar:      "LOGD_FILE",
			Value:       "__log",
			Destination: &config.LogFile,
		}),
		altsrc.NewIntFlag(cli.IntFlag{
			Name:        "log_file_mode",
			Usage:       "Integer representation of file mode",
			EnvVar:      "LOGD_FILE_MODE",
			Value:       0644,
			Destination: &config.LogFileMode,
		}),
		altsrc.NewIntFlag(cli.IntFlag{
			Name:        "max_chunk_size",
			Usage:       "Size, in bytes, of maximum chunk length",
			EnvVar:      "LOGD_MAX_CHUNK_SIZE",
			Value:       1024 * 1024 * 2,
			Destination: &config.MaxChunkSize,
		}),
		altsrc.NewIntFlag(cli.IntFlag{
			Name:   "partition_size",
			Usage:  "Size, in bytes, of partition",
			EnvVar: "LOGD_PARTITION_SIZE",
			// Value:       1024 * 1024 * 500,
			Value:       1024 * 50,
			Destination: &config.PartitionSize,
		}),
		altsrc.NewUint64Flag(cli.Uint64Flag{
			Name:        "index_cursor_size",
			Usage:       "Distance between index entries",
			EnvVar:      "LOGD_INDEX_CURSOR_SIZE",
			Value:       1000,
			Destination: &config.IndexCursorSize,
		}),
		altsrc.NewBoolFlag(cli.BoolFlag{
			Name:        "can_shutdown",
			Usage:       "Server can be shut down via command",
			EnvVar:      "LOGD_CAN_SHUTDOWN",
			Destination: &config.CanShutdown,
		}),

		// TODO make action for this instead of flag
		cli.BoolFlag{
			Name:        "check",
			Usage:       "Check index for errors",
			EnvVar:      "LOGD_CHECK",
			Destination: &check,
		},
	}

	app.Before = altsrc.InitInputSourceWithContext(app.Flags, altsrc.NewYamlSourceFromFlagFunc("config"))

	app.Action = func(c *cli.Context) error {
		if check {
			if err := logd.CheckIndex(config); err != nil {
				panic(err)
			}
		} else {
			srv := logd.NewServer(config.Hostport, config)
			if err := srv.ListenAndServe(); err != nil {
				return err
			}
		}
		return nil
	}

	sort.Sort(cli.FlagsByName(app.Flags))

	if err := app.Run(args); err != nil {
	}
}

func main() {
	runApp(os.Args)
}
