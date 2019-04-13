package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/logd"
)

var CheckIndexCmd = &cobra.Command{
	Use:   "check-index file...",
	Short: "checks an index file",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		internal.Debugf(tmpConfig.ToGeneralConfig(), "%+v", tmpConfig)
		if err := doCheckIndex(tmpConfig, cmd, args); err != nil {
			panic(err)
		}
	},
}

func doCheckIndex(conf *logd.Config, c *cobra.Command, args []string) error {
	fmt.Println(args)
	return nil
}
