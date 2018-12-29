package main

import (
	"fmt"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	pflags := ReadCmd.PersistentFlags()
	dconf := client.DefaultConfig

	pflags.IntVar(&tmpConfig.Limit, "limit", dconf.Limit, "limit minimum number of messages per read to `MESSAGES`")
	pflags.Uint64Var(&tmpConfig.Offset, "offset", dconf.Offset, "start reading messages from `OFFSET`")

	pflags.BoolVarP(&tmpConfig.ReadForever, "read-forever", "F", dconf.WriteForever, "Keep reading input until the program is killed")
	pflags.StringVar(&topicFlag, "topic", "default", "a `TOPIC` for the read")

	pflags.IntVar(&tmpConfig.ConnRetries, "retries", dconf.ConnRetries, "total number of connection retries")
	pflags.DurationVar(&tmpConfig.ConnRetryInterval, "retry-interval", dconf.ConnRetryInterval, "initial retry interval duration")
	pflags.Float64Var(&tmpConfig.ConnRetryMultiplier, "retry-multiplier", dconf.ConnRetryMultiplier, "retry interval multiplier")
	pflags.DurationVar(&tmpConfig.ConnRetryMaxInterval, "retry-max-interval", dconf.ConnRetryMaxInterval, "maximum retry interval")
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

func doRead(conf *client.Config, c *cobra.Command) error {
	done := make(chan struct{})
	handleKills(done)

	conf.UseTail = !c.PersistentFlags().Lookup("offset").Changed
	if conf.ReadForever {
		conf.Limit = 1000
	}

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

	n := 0
	for scanner.Scan() {
		_, err := out.Write(scanner.Message().BodyBytes())
		if err != nil && errors.Cause(err) == protocol.ErrNotFound {
			break
		}
		internal.LogError(err)
		_, err = out.Write([]byte("\n"))
		internal.LogError(err)
		n++
		if !conf.ReadForever && n > conf.Limit {
			break
		}
	}

	if n == 0 {
		fmt.Println(t, "topic is empty")
	}
	return nil
}
