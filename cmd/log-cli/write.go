package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/logd"
	"github.com/spf13/cobra"
)

func init() {
	pflags := WriteCmd.PersistentFlags()
	dconf := logd.DefaultConfig

	pflags.Uint64Var(&tmpConfig.Offset, "offset", dconf.Offset,
		"start reading messages from `OFFSET`")
	pflags.BoolVarP(&tmpConfig.WriteForever, "write-forever", "F", dconf.WriteForever,
		"Keep reading input until the program is killed")
	pflags.StringVar(&topicFlag, "topic", "default",
		"a `TOPIC` for the write")

	pflags.StringVar(&tmpConfig.InputPath, "input", dconf.InputPath,
		"A file path to read messages into the log")
	pflags.StringVar(&tmpConfig.OutputPath, "output", dconf.OutputPath,
		"A file path for writing response offsets")
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

func doWrite(conf *logd.Config, c *cobra.Command, args []string) error {
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

	var m logd.StatePusher
	if out == nil {
		m = &logd.NoopStatePusher{}
	} else {
		m = logd.NewStateOutputter(out)
	}

	w := logd.NewWriter(conf, t).WithStateHandler(m)
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
