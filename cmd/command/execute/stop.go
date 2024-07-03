package execute

import (
	"fmt"

	"github.com/spf13/cobra"
)

// 迁移任务开始
var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the task that is running",
	Long:  "Stop the task that is running.",
	Run: func(cmd *cobra.Command, args []string) {
		//ctx := context.Background()
		// res, err := client.Stop(ctx, &pipe.Empty{})
		// if err != nil {
		// 	unpackGrpcError(cmd, args, err)
		// 	return
		// }
		fmt.Println("==> 已经暂停任务信息：")
		//ExecSuccess(fmt.Sprintf("任务名称：%s,任务当前状态：%s", res.TaskName, res.Status))
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)
}
