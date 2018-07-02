package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	cli "gopkg.in/urfave/cli.v2"
)

var tmpConfig = &client.Config{}

var helpFlag = &cli.BoolFlag{
	Name:    "help",
	Aliases: []string{},
	Usage:   "show help",
}

var versionFlag = &cli.BoolFlag{
	Name:    "version",
	Aliases: []string{},
	Usage:   "Print version",
}

var verboseFlag = &cli.BoolFlag{
	Name:        "verbose",
	Aliases:     []string{"v"},
	Usage:       "Print debug information to the console",
	Value:       client.DefaultConfig.Verbose,
	EnvVars:     []string{"LOG_VERBOSE"},
	Destination: &tmpConfig.Verbose,
}

var hostFlag = &cli.StringFlag{
	Name:        "host",
	Aliases:     []string{"h"},
	Usage:       "A `HOST:PORT` combination to connect to a logd instance",
	Value:       client.DefaultConfig.Hostport,
	EnvVars:     []string{"LOG_HOST"},
	Destination: &tmpConfig.Hostport,
}

var timeoutFlag = &cli.DurationFlag{
	Name:        "timeout",
	Aliases:     []string{"t"},
	Usage:       "`DURATION` to wait for reads and writes to complete",
	Value:       client.DefaultConfig.Timeout,
	EnvVars:     []string{"LOG_TIMEOUT"},
	Destination: &tmpConfig.Timeout,
}

var writeTimeoutFlag = &cli.DurationFlag{
	Name:        "write-timeout",
	Usage:       "`DURATION` to wait for writes to complete",
	Value:       client.DefaultConfig.WriteTimeout,
	EnvVars:     []string{"LOG_WRITE_TIMEOUT"},
	Destination: &tmpConfig.WriteTimeout,
}

var readTimeoutFlag = &cli.DurationFlag{
	Name:        "read-timeout",
	Usage:       "`DURATION` to wait for reads to complete",
	Value:       client.DefaultConfig.ReadTimeout,
	EnvVars:     []string{"LOG_READ_TIMEOUT"},
	Destination: &tmpConfig.ReadTimeout,
}

var batchSizeFlag = &cli.IntFlag{
	Name:        "batch-size",
	Aliases:     []string{"b"},
	Usage:       "max size of batch in `BYTES`",
	Value:       client.DefaultConfig.BatchSize,
	EnvVars:     []string{"LOG_BATCH_SIZE"},
	Destination: &tmpConfig.BatchSize,
}

var limitFlag = &cli.IntFlag{
	Name:        "limit",
	Aliases:     []string{"l"},
	Usage:       "limit minimum number of messages per read to `MESSAGES`",
	Value:       client.DefaultConfig.Limit,
	EnvVars:     []string{"LOG_LIMIT"},
	Destination: &tmpConfig.Limit,
}

var offsetFlag = &cli.Uint64Flag{
	Name:        "offset",
	Aliases:     []string{"s"},
	Usage:       "start reading messages from `OFFSET`",
	Value:       client.DefaultConfig.Offset,
	EnvVars:     []string{"LOG_OFFSET"},
	Destination: &tmpConfig.Offset,
}

var waitIntervalFlag = &cli.DurationFlag{
	Name:        "wait-interval",
	Usage:       "`DURATION` to wait after the last write to flush the current batch",
	Value:       client.DefaultConfig.WaitInterval,
	EnvVars:     []string{"LOG_WAIT_INTERVAL"},
	Destination: &tmpConfig.WaitInterval,
}

var writeForeverFlag = &cli.BoolFlag{
	Name:        "write-forever",
	Aliases:     []string{"F"},
	Usage:       "Keep reading input until the program is killed",
	Value:       client.DefaultConfig.WriteForever,
	EnvVars:     []string{"LOG_WRITE_FOREVER"},
	Destination: &tmpConfig.WriteForever,
}

var inputPathFlag = &cli.PathFlag{
	Name:        "input",
	Usage:       "A file path to read messages into the log",
	Value:       client.DefaultConfig.InputPath,
	EnvVars:     []string{"LOG_INPUT"},
	Destination: &tmpConfig.InputPath,
}

var outputPathFlag = &cli.PathFlag{
	Name:        "output",
	Usage:       "A file path to write offsets to",
	Value:       client.DefaultConfig.OutputPath,
	EnvVars:     []string{"LOG_OUTPUT"},
	Destination: &tmpConfig.OutputPath,
}

var countFlag = &cli.BoolFlag{
	Name:        "count",
	Aliases:     []string{"c"},
	Usage:       "Print counts before exiting",
	Value:       client.DefaultConfig.Count,
	EnvVars:     []string{"LOG_COUNT"},
	Destination: &tmpConfig.Count,
}

var sharedFlags = []cli.Flag{
	verboseFlag, hostFlag,
	timeoutFlag, writeTimeoutFlag, readTimeoutFlag,
	waitIntervalFlag, outputPathFlag,
	countFlag,
}

func withSharedFlags(flags []cli.Flag) []cli.Flag {
	res := make([]cli.Flag, len(sharedFlags))
	copy(res, sharedFlags)
	return append(res, flags...)
}

func buildConfig(c *cli.Context) *client.Config {
	conf := &client.Config{}
	*conf = *tmpConfig
	internal.Debugf(&config.Config{Verbose: conf.Verbose}, "%v", conf)
	return conf
}

func getFile(s string, in bool) (*os.File, error) {
	if s == "-" || s == "" {
		f := os.Stdout
		if in {
			f = os.Stdin
		}
		stat, err := f.Stat()
		if err != nil {
			return nil, err
		}
		if in && (stat.Mode()&os.ModeCharDevice) != 0 {
			return nil, nil
		}
		return f, nil
	}
	return os.Open(s)
}

func handleKills(c chan struct{}) {
	stopC := make(chan os.Signal)
	signal.Notify(stopC, os.Interrupt, syscall.SIGTERM)
	go func() {
		for range stopC {
			c <- struct{}{}
		}
	}()
}

func prettyNumBytes(n float64) string {
	if n > 1024*1024 {
		return fmt.Sprintf("%.2fMb", n/1024/1024)
	}
	if n > 1024 {
		return fmt.Sprintf("%.2fKb", n/1024)
	}
	return fmt.Sprintf("%.1fb", n)
}

func prettyNum(n float64) string {
	if n > 1000*1000 {
		return fmt.Sprintf("%.2fM", n/1000/1000)
	}
	if n > 1000 {
		return fmt.Sprintf("%.2fK", n/1000)
	}
	return fmt.Sprintf("%.0f", n)
}

func fill(s string, l int) string {
	for len(s) < l {
		s += " "
	}
	return s
}

type debugCounts struct {
	started time.Time
	counts  map[string]int
}

func newDebugCounts(started time.Time) *debugCounts {
	return &debugCounts{
		started: started,
		counts: map[string]int{
			"messages": 0,
			"in":       0,
		},
	}
}

func (c *debugCounts) String() string {
	// return fmt.Sprintf("")
	b := bytes.Buffer{}
	dur := time.Now().Sub(c.started)
	b.WriteString(fmt.Sprintf("elapsed :\t\t%s\n", dur))
	for k, v := range c.counts {
		var formatted string
		var formattedPer string
		if k == "in" {
			formatted = prettyNumBytes(float64(v))
			formattedPer = prettyNumBytes(float64(v) / float64(dur.Seconds()))
		} else {
			formatted = prettyNum(float64(v))
			formattedPer = prettyNum(float64(v) / float64(dur.Seconds()))
		}

		b.WriteString(fmt.Sprintf("%s:\t\t%s\t\t%s %s/s\n",
			fill(k, 10), formatted, formattedPer, k))
	}
	return b.String()
}

func doWrite(conf *client.Config, args cli.Args) error {
	done := make(chan struct{})
	handleKills(done)

	started := time.Now()
	counts := newDebugCounts(started)

	in, err := getFile(conf.InputPath, true)
	if err != nil {
		return err
	}
	if in != nil {
		defer in.Close()
	}
	out, err := getFile(conf.OutputPath, false)
	if err != nil {
		return err
	}
	if out != nil {
		defer out.Close()
	}

	w, err := client.DialWriterConfig(conf.Hostport, conf)
	if err != nil {
		return err
	}
	defer w.Close()

	var m client.StatePusher
	if out == nil {
		m = &client.NoopStatePusher{}
	} else {
		m = client.NewStateOutputter(out)
	}
	w.SetStateHandler(m)

	for _, arg := range args.Slice() {
		select {
		case <-done:
			break
		default:
		}

		if len(arg) == 0 {
			continue
		}

		n, err := w.Write([]byte(arg))
		counts.counts["in"] += n
		if err != nil {
			return err
		}
		counts.counts["messages"]++
	}

	// writer reads lines from stdin or the specified file
	if in != nil {
		scanner := bufio.NewScanner(in)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			select {
			case <-done:
				break
			default:
			}

			b := scanner.Bytes()
			if len(b) == 0 {
				continue
			}

			n, err := w.Write(internal.CopyBytes(b))
			counts.counts["in"] += n
			if err != nil {
				return err
			}
			counts.counts["messages"]++
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}

	if conf.Count {
		_, err := fmt.Fprintln(os.Stderr, counts)
		internal.LogError(err)
	}
	return w.Flush()
}

func doRead(conf *client.Config) error {
	done := make(chan struct{})
	handleKills(done)

	scanner, err := client.DialScannerConfig(conf.Hostport, conf)
	if err != nil {
		return err
	}
	defer scanner.Close()

	for scanner.Scan() {
		fmt.Printf("%q\n", scanner.Message().BodyBytes())
	}

	return nil
}

func runApp(args []string) {
	cli.HelpFlag = helpFlag
	cli.VersionFlag = versionFlag
	app := &cli.App{
		Name:                  "log-cli",
		Usage:                 "logd command-line client",
		Version:               "0.0.1",
		EnableShellCompletion: true,
		Flags: withSharedFlags([]cli.Flag{}),
		Commands: []*cli.Command{
			{
				Name:    "write",
				Aliases: []string{"w"},
				Usage:   "writes messages to the log",
				Flags: withSharedFlags([]cli.Flag{
					batchSizeFlag,
					writeForeverFlag,
					inputPathFlag,
				}),
				Action: func(c *cli.Context) error {
					return doWrite(buildConfig(c), c.Args())
				},
			},
			{
				Name:    "read",
				Aliases: []string{"r"},
				Usage:   "reads messages from the log",
				Flags: withSharedFlags([]cli.Flag{
					batchSizeFlag,
					limitFlag, offsetFlag,
				}),
				Action: func(c *cli.Context) error {
					return doRead(buildConfig(c))
				},
			},
		},
	}

	if err := app.Run(args); err != nil {
		panic(err)
	}
}

func main() {
	runApp(os.Args)
}
