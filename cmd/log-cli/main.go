package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/jeffrom/logd"
	"github.com/urfave/cli"
)

func toBytes(args []string) [][]byte {
	var b [][]byte
	for _, arg := range args {
		b = append(b, []byte(arg))
	}

	return b
}

func cmdAction(config *logd.Config, cmd logd.CmdType) func(c *cli.Context) error {
	return func(c *cli.Context) error {
		client, err := logd.DialConfig(config.Hostport, config)
		if err != nil {
			return cli.NewExitError(err, 1)
		}

		resp, err := client.Do(logd.NewCommand(cmd, toBytes(c.Args())...))
		if err != nil {
			return cli.NewExitError(err, 1)
		}

		fmt.Printf("%s\n", bytes.Trim(resp.Bytes(), "\r\n"))

		if resp.Status == logd.RespErr {
			return cli.NewExitError("Server error", 2)
		}
		if resp.Status == logd.RespErrClient {
			return cli.NewExitError("Client error", 3)
		}
		return nil
	}
}

func main() {
	config := &logd.Config{}
	*config = *logd.DefaultConfig

	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "Print only the version",
	}

	app := cli.NewApp()
	app.Name = "log-cli"
	app.Usage = "networked logging client"
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
			Usage:       "Time, in milliseconds, to wait for a response",
			EnvVar:      "LOGD_TIMEOUT",
			Value:       500,
			Destination: &config.ClientTimeout,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "ping",
			Usage:  "ping a host for availability",
			Action: cmdAction(config, logd.CmdPing),
		},
		{
			Name:   "sleep",
			Usage:  "pause the current transaction",
			Action: cmdAction(config, logd.CmdSleep),
		},
		{
			Name:   "head",
			Usage:  "get the log's current ID",
			Action: cmdAction(config, logd.CmdHead),
		},
		{
			Name:   "msg",
			Usage:  "write a message to the log",
			Action: cmdAction(config, logd.CmdMessage),
		},
	}

	if err := app.Run(os.Args); err != nil {
	}
}
