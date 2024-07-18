package execute

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	VERSION = "1.0.0"
)

var versionCmd = &cobra.Command{
	Use:     "version",
	Aliases: []string{"v"},
	Short:   "print the version of obsync",
	Long:    "print the version of obsync...",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintln(os.Stdout, "obsync CLI Tool: "+VERSION)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
