package main

import (
	"fmt"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/internal"
	"github.com/spf13/cobra"
)

var ConfigCmd = &cobra.Command{
	Use:     "config",
	Aliases: []string{"conf"},
	Short:   "Read server config",
	Long:    ``,
	Run: func(cmd *cobra.Command, args []string) {
		internal.Debugf(tmpConfig.ToGeneralConfig(), "%+v", tmpConfig)
		c := client.New(tmpConfig)
		serverConf, err := c.Config()
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", serverConf)
	},
}
