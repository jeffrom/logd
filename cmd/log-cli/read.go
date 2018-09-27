package main

import (
	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/internal"
	"github.com/spf13/cobra"
)

func init() {
	ReadCmd.PersistentFlags().IntVar(&tmpConfig.Limit, "limit", client.DefaultConfig.Limit, "limit minimum number of messages per read to `MESSAGES`")
	ReadCmd.PersistentFlags().Uint64Var(&tmpConfig.Offset, "offset", client.DefaultConfig.Offset, "start reading messages from `OFFSET`")

	ReadCmd.PersistentFlags().BoolVarP(&tmpConfig.ReadForever, "read-forever", "F", client.DefaultConfig.WriteForever, "Keep reading input until the program is killed")
	ReadCmd.PersistentFlags().StringVar(&topicFlag, "topic", "default", "a `TOPIC` for the read")
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
	conf.UseTail = !c.PersistentFlags().Lookup("offset").Changed
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

	n := 0
	for scanner.Scan() {
		_, err := out.Write(scanner.Message().BodyBytes())
		internal.LogError(err)
		_, err = out.Write([]byte("\n"))
		internal.LogError(err)
		n++
		if n > conf.Limit {
			break
		}
	}

	return nil
}
