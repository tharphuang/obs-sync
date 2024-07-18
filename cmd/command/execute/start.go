package execute

import (
	"context"
	"obs-sync/pkg/utils"
	"obs-sync/proto/sync/pb"

	"github.com/spf13/cobra"
)

// 迁移任务开始
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start one rsync task.",
	Long:  "Start one rsync task...",
	Run: func(cmd *cobra.Command, args []string) {
		stream, err := client.Start(context.Background(), &pb.Empty{})
		if err != nil {
			unpackGrpcError(cmd, args, err)
		}
		progress := utils.NewProgress(false, true)
		Handled := progress.AddCountBar("Scanned objects", 0)
		Copied := progress.AddCountSpinner("Copied objects")
		CopiedBytes := progress.AddByteSpinner("Copied objects")
		Skipped := progress.AddCountSpinner("Skipped objects")
		Failed := progress.AddCountSpinner("Failed objects")
		for {
			resp, err := stream.Recv()
			if err != nil {
				progress.Quiet = true
				unpackGrpcError(cmd, args, err)
				return
			}
			v := resp.Value
			//statsValue, statsType := v>>4, models.StatsType(v&(15))
			Handled.SetTotal(v.Scanned)
			Handled.SetCurrent(v.Copied + v.Failed + v.Skipped)
			Copied.SetCurrent(v.Copied)
			Skipped.SetCurrent(v.Skipped)
			Failed.SetCurrent(v.Failed)
			CopiedBytes.SetCurrent(v.Size)

			if v.FinishFlag {
				ExecSuccess("对象同步已完成,无需同步")
				return
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
}
