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
	Short:   "Print the version of ossImport",
	Long:    "Print the version of ossImport...",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintln(os.Stdout, "CuCloudOssImport CLI Tool: "+VERSION)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
