package main

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeffrom/logd/logd"
	"github.com/spf13/cobra"
)

var (
	ReleaseVersion = "none"
	ReleaseCommit  = "none"
	ReleaseDate    = "none"
)

var tmpConfig = &logd.Config{}
var topicFlag string

func init() {
	pflags := RootCmd.PersistentFlags()

	pflags.BoolVarP(&tmpConfig.Verbose, "verbose", "v", logd.DefaultConfig.Verbose, "print debug output")
	pflags.StringVar(&tmpConfig.Host, "host", logd.DefaultConfig.Host, "a `HOST:PORT` combination to listen on")
	pflags.DurationVar(&tmpConfig.Timeout, "timeout", logd.DefaultConfig.Timeout, "duration to wait for requests to complete")
	pflags.DurationVar(&tmpConfig.ConnectTimeout, "connect-timeout", logd.DefaultConfig.ConnectTimeout, "duration to wait for connection to establish. Overrides 'timeout' if set")
	pflags.DurationVar(&tmpConfig.WriteTimeout, "write-timeout", logd.DefaultConfig.WriteTimeout, "duration to wait for writes to the server to complete. Overrides 'timeout' if set")
	pflags.DurationVar(&tmpConfig.ReadTimeout, "read-timeout", logd.DefaultConfig.ReadTimeout, "duration to wait for reads from the server to complete. Overrides 'timeout' if set")
	pflags.IntVar(&tmpConfig.BatchSize, "batch-size", logd.DefaultConfig.BatchSize, "maximum size of batch in bytes")
	pflags.DurationVar(&tmpConfig.WaitInterval, "wait-interval", logd.DefaultConfig.WaitInterval, "duration to wait after the last write to flush the current batch")
	pflags.BoolVarP(&tmpConfig.Count, "count", "c", logd.DefaultConfig.Count, "Print counts before exiting")
}

var RootCmd = &cobra.Command{
	Use:   "log-cli",
	Short: "log-cli - networked log transport command-line client",
	Long:  ``,
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
	if n > 1024*1024*1024 {
		return fmt.Sprintf("%.2fGb", n/1024/1024/1024)
	}
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

func main() {
	RootCmd.AddCommand(WriteCmd)
	RootCmd.AddCommand(ReadCmd)
	RootCmd.AddCommand(ConfigCmd)
	RootCmd.AddCommand(BenchCmd)
	RootCmd.AddCommand(VersionCmd)

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
