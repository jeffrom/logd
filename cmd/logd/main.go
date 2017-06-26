package main

import (
	"os"
	"sort"

	"github.com/urfave/cli"

	"github.com/jeffrom/logd"
)

func main() {
	config := &logd.Config{}
	*config = *logd.DefaultConfig

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
			EnvVar: "LOGD_CONFIG",
		},
		cli.BoolFlag{
			Name:        "verbose, v",
			Usage:       "print debug output",
			EnvVar:      "LOGD_VERBOSE",
			Destination: &config.Verbose,
		},
		cli.StringFlag{
			Name:        "host",
			Usage:       "A `HOST:PORT` combination to connect to",
			EnvVar:      "LOGD_HOST",
			Value:       "127.0.0.1:1774",
			Destination: &config.Hostport,
		},
		cli.UintFlag{
			Name:        "timeout",
			Usage:       "Time, in milliseconds, to wait for a response to be acknowledged",
			EnvVar:      "LOGD_TIMEOUT",
			Value:       500,
			Destination: &config.ServerTimeout,
		},
	}

	app.Action = func(c *cli.Context) error {
		srv := logd.NewServer(config.Hostport, config)
		if err := srv.ListenAndServe(); err != nil {
			return err
		}
		return nil
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	if err := app.Run(os.Args); err != nil {
	}
}
