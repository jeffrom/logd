package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/urfave/cli.v1"
	"gopkg.in/urfave/cli.v1/altsrc"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

func toBytes(args []string) [][]byte {
	var b [][]byte
	for _, arg := range args {
		b = append(b, []byte(arg))
	}

	return b
}

func checkErrResp(resp *protocol.Response) error {
	if resp.Status == protocol.RespErr {
		return cli.NewExitError("Server error", 2)
	}
	if resp.Status == protocol.RespErrClient {
		return cli.NewExitError("Client error", 3)
	}
	return nil
}

func formatResp(resp *protocol.Response, args []string) string {
	var out bytes.Buffer
	respBytes, err := resp.SprintBytes()
	internal.PanicOnError(err)

	isOk := bytes.HasPrefix(respBytes, []byte("OK "))
	respBytes = bytes.TrimLeft(respBytes, "OK ")

	idx := bytes.Index(respBytes, []byte(" "))
	if idx > 0 {
		respBytes = respBytes[idx+1:]
	} else if isOk && args != nil {
		var lastID uint64
		if _, err := fmt.Sscanf(string(respBytes), "%d", &lastID); err != nil {
			panic(err)
		}

		for i := lastID - uint64(len(args)-1); i < lastID; i++ {
			out.WriteString(fmt.Sprintf("%d\n", i))
		}
	}
	out.Write(bytes.TrimRight(respBytes, "\r\n"))
	return out.String()
}

func cmdAction(conf *config.Config, cmd protocol.CmdType) func(c *cli.Context) error {
	return func(cliCtx *cli.Context) error {
		c, err := client.DialConfig(conf.Hostport, conf)
		if err != nil {
			return cli.NewExitError(err, 1)
		}
		defer c.Close()

		if len(cliCtx.Args()) > 0 {
			resp, err := c.Do(protocol.NewCommand(conf, cmd, toBytes(cliCtx.Args())...))
			if err != nil {
				return cli.NewExitError(err, 1)
			}
			fmt.Println(formatResp(resp, cliCtx.Args()))
			if err := checkErrResp(resp); err != nil {
				return err
			}
		} else if cmd != protocol.CmdMessage {
			resp, err := c.Do(protocol.NewCommand(conf, cmd))
			if err != nil {
				if err == io.EOF && cmd == protocol.CmdShutdown {
					return nil
				}
				return cli.NewExitError(err, 1)
			}
			fmt.Println(formatResp(resp, cliCtx.Args()))
			if err := checkErrResp(resp); err != nil {
				return err
			}
		}

		// check if there's data in stdin
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			return nil
		}

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Split(bufio.ScanLines)
		var lastResp *protocol.Response
		for scanner.Scan() {
			b := scanner.Bytes()
			resp, err := c.Do(protocol.NewCommand(conf, cmd, b))
			lastResp = resp
			if err == io.EOF {
				return nil
			}
			if err != nil {
				panic(err)
			}
			if cmd != protocol.CmdMessage {
				fmt.Println(formatResp(resp, nil))
			}
			if err := checkErrResp(resp); err != nil {
				return err
			}
		}

		if cmd == protocol.CmdMessage {
			fmt.Println(formatResp(lastResp, nil))
		}
		return nil
	}
}

func doReadCmdAction(conf *config.Config) func(c *cli.Context) error {
	return func(cliCtx *cli.Context) error {
		start := conf.StartID
		limit := int(conf.ReadLimit)

		c, err := client.DialConfig(conf.Hostport, conf)
		if err != nil {
			log.Printf("%+v", err)
			return cli.NewExitError(err, 1)
		}
		defer c.Close()

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

		if start == 0 {
			resp, headErr := c.Do(protocol.NewCommand(conf, protocol.CmdHead))
			if err != nil {
				log.Printf("%+v", err)
				return cli.NewExitError(headErr, 2)
			}

			if resp.ID < uint64(limit) {
				limit = int(resp.ID)
				start = 1
			} else {
				start = resp.ID - uint64(limit) + 1
			}
		} else if start > 0 {
			// limit--
		}

		if conf.ReadForever {
			limit = 0
		}

		// fmt.Printf("Reading %d messages from id %d\n", limit, start)

		scanner, err := c.DoRead(start, limit)
		if err != nil {
			log.Printf("%+v", err)
			return cli.NewExitError(err, 2)
		}

		timeout := time.Duration(conf.ClientTimeout) * time.Millisecond
		if srerr := c.SetReadDeadline(time.Now().Add(timeout)); srerr != nil {
			panic(srerr)
		}
		for scanner.Scan() {
			msg := scanner.Message()
			fmt.Printf("%d %s\n", msg.ID, msg.Body)
		}

		if err := scanner.Error(); err != io.EOF && err != nil {
			if cerr, ok := errors.Cause(err).(net.Error); !ok || !cerr.Timeout() {
				log.Printf("%+v", err)
				return cli.NewExitError(err, 3)
			}
		}

		if conf.ReadForever {
			for {
				// fmt.Printf("waiting for more log entries\n")

				select {
				case <-sigc:
					return nil
				case <-time.After(200 * time.Millisecond):
				}

				c.SetDeadline(time.Now().Add(200 * time.Millisecond))
				for scanner.Scan() {
					msg := scanner.Message()
					fmt.Printf("%d %s\n", msg.ID, msg.Body)
					c.SetDeadline(time.Now().Add(200 * time.Millisecond))
				}

				if err := scanner.Error(); err != nil && err != io.EOF {
					if cerr, ok := errors.Cause(err).(net.Error); !ok || !cerr.Timeout() {
						log.Printf("%+v", err)
						return cli.NewExitError(err, 3)
					}
				}

			}
		}

		return nil
	}
}

func runApp(args []string) {
	conf := &config.Config{}
	*conf = *config.DefaultConfig

	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "Print only the version",
	}

	app := cli.NewApp()
	app.Name = "log-cli"
	app.Usage = "networked logging client"
	app.Version = "0.0.1"
	app.EnableBashCompletion = true

	flags := []cli.Flag{
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
			Usage:       "`MILLISECONDS` to wait for a response",
			EnvVar:      "LOGD_TIMEOUT",
			Value:       500,
			Destination: &conf.ClientTimeout,
		}),
	}

	app.Flags = flags

	app.Before = altsrc.InitInputSourceWithContext(app.Flags, altsrc.NewYamlSourceFromFlagFunc("config"))

	app.Commands = []cli.Command{
		{
			Name:   "ping",
			Usage:  "ping a host for availability",
			Action: cmdAction(conf, protocol.CmdPing),
		},
		{
			Name:   "sleep",
			Usage:  "pause the current transaction",
			Action: cmdAction(conf, protocol.CmdSleep),
		},
		{
			Name:   "head",
			Usage:  "get the log's current ID",
			Action: cmdAction(conf, protocol.CmdHead),
		},
		{
			Name:   "write",
			Usage:  "write a message to the log",
			Action: cmdAction(conf, protocol.CmdMessage),
		},
		{
			Name:  "read",
			Usage: "read from the log",
			Flags: append([]cli.Flag{
				cli.Uint64Flag{
					Name:        "start, s",
					Usage:       "Starting `ID`",
					Value:       0,
					Destination: &conf.StartID,
				},
				cli.Uint64Flag{
					Name:        "limit, n",
					Usage:       "`NUMBER` of message to read",
					Value:       15,
					Destination: &conf.ReadLimit,
				},
				cli.BoolFlag{
					Name:        "forever, f",
					Usage:       "read forever",
					Destination: &conf.ReadForever,
				},
				cli.BoolFlag{
					Name:        "from-tail, t",
					Usage:       "read from log tail if the queried id can't be found",
					Destination: &conf.ReadFromTail,
				},
			}, flags...),
			Action: doReadCmdAction(conf),
		},
		{
			Name:   "stats",
			Usage:  "get running server stats",
			Action: cmdAction(conf, protocol.CmdStats),
		},
		{
			Name:   "shutdown",
			Usage:  "shutdown the server (debug only)",
			Action: cmdAction(conf, protocol.CmdShutdown),
		},
	}

	if err := app.Run(args); err != nil {
		log.Printf("closed: %+v", err)
	}
}

func main() {
	runApp(os.Args)
}
