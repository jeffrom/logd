package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"

	"gopkg.in/urfave/cli.v1"
	"gopkg.in/urfave/cli.v1/altsrc"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/events"
	"github.com/jeffrom/logd/internal"
)

func runApp(args []string) {
	conf := &config.Config{}
	*conf = *config.Default

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
			Value:  "logd.conf.yml",
			EnvVar: "LOGD_CONFIG",
		},
		altsrc.NewBoolFlag(cli.BoolFlag{
			Name:        "verbose, v",
			Usage:       "print debug output",
			EnvVar:      "LOGD_VERBOSE",
			Destination: &conf.Verbose,
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:        "host",
			Usage:       "A `HOST:PORT` combination to connect to",
			EnvVar:      "LOGD_HOST",
			Value:       "127.0.0.1:1774",
			Destination: &conf.Hostport,
		}),
		altsrc.NewUintFlag(cli.UintFlag{
			Name:        "timeout",
			Usage:       "Time, in milliseconds, to wait for a response to be acknowledged",
			EnvVar:      "LOGD_TIMEOUT",
			Value:       config.Default.ServerTimeout,
			Destination: &conf.ServerTimeout,
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:        "log_file",
			Usage:       "Log file name",
			EnvVar:      "LOGD_FILE",
			Value:       config.Default.WorkDir,
			Destination: &conf.WorkDir,
		}),
		altsrc.NewIntFlag(cli.IntFlag{
			Name:        "log_file_mode",
			Usage:       "Integer representation of file mode",
			EnvVar:      "LOGD_FILE_MODE",
			Value:       config.Default.LogFileMode,
			Destination: &conf.LogFileMode,
		}),
		altsrc.NewIntFlag(cli.IntFlag{
			Name:        "max_batch_size",
			Usage:       "Size, in bytes, of maximum chunk length",
			EnvVar:      "LOGD_MAX_BATCH_SIZE",
			Value:       config.Default.MaxBatchSize,
			Destination: &conf.MaxBatchSize,
		}),
		altsrc.NewIntFlag(cli.IntFlag{
			Name:        "partition_size",
			Usage:       "Size, in bytes, of partition",
			EnvVar:      "LOGD_PARTITION_SIZE",
			Value:       config.Default.PartitionSize,
			Destination: &conf.PartitionSize,
		}),
		altsrc.NewIntFlag(cli.IntFlag{
			Name:        "max_partitions",
			Usage:       "number of partitions to save on disk",
			EnvVar:      "LOGD_MAX_PARTITIONS",
			Value:       config.Default.MaxPartitions,
			Destination: &conf.MaxPartitions,
		}),
		altsrc.NewBoolFlag(cli.BoolFlag{
			Name:        "can_shutdown",
			Usage:       "Server can be shut down via command",
			EnvVar:      "LOGD_CAN_SHUTDOWN",
			Destination: &conf.CanShutdown,
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
			panic("not implemented")
		}

		q := events.NewEventQ(conf)

		stopC := make(chan os.Signal, 1)
		signal.Notify(stopC, os.Interrupt, syscall.SIGTERM)
		go func() {
			for range stopC {
				log.Print("Caught signal. Exiting...")
				internal.LogError(q.Stop())
			}
		}()

		go func() {
			runtime.SetBlockProfileRate(10000)
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()

		return q.Start()
	}

	sort.Sort(cli.FlagsByName(app.Flags))

	if err := app.Run(args); err != nil {
		panic(err)
	}
}

func main() {
	runApp(os.Args)
}
