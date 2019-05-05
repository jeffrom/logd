package main

import (
	"fmt"

	"github.com/jeffrom/logd/internal"
	"github.com/spf13/cobra"
)

var VersionCmd = &cobra.Command{
	Use:     "version",
	Aliases: []string{"v"},
	Short:   "Print version and exit",
	Long:    ``,
	Run: func(cmd *cobra.Command, args []string) {
		internal.Debugf(tmpConfig.ToGeneralConfig(), "%+v", tmpConfig)
		fmt.Printf("version: %s, released: %s, commit: %s\n",
			ReleaseVersion, ReleaseDate, ReleaseCommit)
	},
}
