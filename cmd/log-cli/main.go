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
	"github.com/jeffrom/logd/internal"
	"github.com/spf13/cobra"
)

var tmpConfig = &client.Config{}
var topicFlag string

func init() {
	pflags := RootCmd.PersistentFlags()

	pflags.BoolVarP(&tmpConfig.Verbose, "verbose", "v", client.DefaultConfig.Verbose, "print debug output")
	pflags.StringVar(&tmpConfig.Hostport, "host", client.DefaultConfig.Hostport, "a `HOST:PORT` combination to listen on")
	pflags.DurationVar(&tmpConfig.Timeout, "timeout", client.DefaultConfig.Timeout, "duration to wait for requests to complete")
	pflags.DurationVar(&tmpConfig.WriteTimeout, "write-timeout", client.DefaultConfig.WriteTimeout, "duration to wait for writes to the server to complete. Overwrites 'timeout' if set")
	pflags.DurationVar(&tmpConfig.ReadTimeout, "read-timeout", client.DefaultConfig.ReadTimeout, "duration to wait for reads from the server to complete. Overwrites 'timeout' if set")
	pflags.IntVar(&tmpConfig.BatchSize, "batch-size", client.DefaultConfig.BatchSize, "maximum size of batch in bytes")
	ReadCmd.PersistentFlags().IntVar(&tmpConfig.Limit, "limit", client.DefaultConfig.Limit, "limit minimum number of messages per read to `MESSAGES`")
	WriteCmd.PersistentFlags().Uint64Var(&tmpConfig.Offset, "offset", client.DefaultConfig.Offset, "start reading messages from `OFFSET`")
	ReadCmd.PersistentFlags().Uint64Var(&tmpConfig.Offset, "offset", client.DefaultConfig.Offset, "start reading messages from `OFFSET`")
	pflags.DurationVar(&tmpConfig.WaitInterval, "wait-interval", client.DefaultConfig.WaitInterval, "duration to wait after the last write to flush the current batch")

	WriteCmd.PersistentFlags().BoolVarP(&tmpConfig.WriteForever, "write-forever", "F", client.DefaultConfig.WriteForever, "Keep reading input until the program is killed")
	ReadCmd.PersistentFlags().BoolVarP(&tmpConfig.ReadForever, "read-forever", "F", client.DefaultConfig.WriteForever, "Keep reading input until the program is killed")

	WriteCmd.PersistentFlags().StringVar(&topicFlag, "topic", "default", "a `TOPIC` for the write")
	ReadCmd.PersistentFlags().StringVar(&topicFlag, "topic", "default", "a `TOPIC` for the read")

	WriteCmd.PersistentFlags().StringVar(&tmpConfig.InputPath, "input", client.DefaultConfig.InputPath, "A file path to read messages into the log")
	WriteCmd.PersistentFlags().StringVar(&tmpConfig.OutputPath, "output", client.DefaultConfig.OutputPath, "A file path for writing response offsets")

	pflags.BoolVarP(&tmpConfig.Count, "count", "c", client.DefaultConfig.Count, "Print counts before exiting")
}

var RootCmd = &cobra.Command{
	Use:   "log-cli",
	Short: "log-cli - networked log transport command-line client",
	Long:  ``,
}

var WriteCmd = &cobra.Command{
	Use:     "write [messages]",
	Aliases: []string{"w"},
	Short:   "Write messages to the log",
	Long:    ``,
	Run: func(cmd *cobra.Command, args []string) {
		internal.Debugf(tmpConfig.ToGeneralConfig(), "%+v", tmpConfig)
		if err := doWrite(tmpConfig, cmd, args); err != nil {
			panic(err)
		}
	},
}

var ReadCmd = &cobra.Command{
	Use:     "read",
	Aliases: []string{"r"},
	Short:   "Read messages from the log",
	Long:    ``,
	Run: func(cmd *cobra.Command, args []string) {
		internal.Debugf(tmpConfig.ToGeneralConfig(), "%+v", tmpConfig)
		if err := doRead(tmpConfig, cmd); err != nil {
			panic(err)
		}
	},
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

var countByteTypes = map[string]bool{
	"in": true,
}

func (c *debugCounts) String() string {
	// return fmt.Sprintf("")
	b := bytes.Buffer{}
	dur := time.Now().Sub(c.started)
	b.WriteString(fmt.Sprintf("elapsed :\t\t%s\n", dur))
	for k, v := range c.counts {
		var formatted string
		var formattedPer string
		if _, ok := countByteTypes[k]; ok {
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

func doWrite(conf *client.Config, c *cobra.Command, args []string) error {
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

	t, err := c.PersistentFlags().GetString("topic")
	if err != nil {
		panic(err)
	}

	var m client.StatePusher
	if out == nil {
		m = &client.NoopStatePusher{}
	} else {
		m = client.NewStateOutputter(out)
	}

	w := client.NewWriter(conf, t).WithStateHandler(m)
	defer w.Close()

	for _, arg := range args {
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

func doRead(conf *client.Config, c *cobra.Command) error {
	conf.UseTail = !c.PersistentFlags().Lookup("offset").Changed
	// if conf.ReadForever {
	// 	conf.Limit = 1000
	// }
	done := make(chan struct{})
	handleKills(done)

	out, err := getFile(conf.OutputPath, false)
	if err != nil {
		return err
	}
	if out != nil {
		defer out.Close()
	}

	scanner, err := client.DialScannerConfig(conf.Hostport, conf)
	if err != nil {
		return err
	}
	defer scanner.Close()

	t, err := c.PersistentFlags().GetString("topic")
	if err != nil {
		panic(err)
	}
	scanner.SetTopic(t)

	go func() {
		select {
		case <-done:
			scanner.Stop()
		}
	}()

	for scanner.Scan() {
		_, err := out.Write(scanner.Message().BodyBytes())
		internal.LogError(err)
		_, err = out.Write([]byte("\n"))
		internal.LogError(err)
	}

	return nil
}

func main() {
	RootCmd.AddCommand(WriteCmd)
	RootCmd.AddCommand(ReadCmd)

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
