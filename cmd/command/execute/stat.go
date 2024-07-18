package execute

import (
	"context"
	"obs-sync/pkg/utils"
	"obs-sync/proto/sync/pb"

	"github.com/spf13/cobra"
)

var StatCmd = &cobra.Command{
	Use:   "stat",
	Short: "Statistics the status of all task",
	Long:  "Statistics the status of all task.",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		res, err := client.Stat(ctx, &pb.Empty{})
		if err != nil {
			ExecError(cmd, args, err.Error())
			return
		}

		progress := utils.NewProgress(false, true)
		Bucket := progress.AddCountBar("Synced buckets", 0)
		Handled := progress.AddCountBar("Scanned objects", 0)
		Copied := progress.AddCountSpinner("Copied objects")
		CopiedBytes := progress.AddByteSpinner("Copied objects")
		Skipped := progress.AddCountSpinner("Skipped objects")
		Failed := progress.AddCountSpinner("Failed objects")

		for {
			recv, err := res.Recv()
			if err != nil {
				progress.Quiet = true
				ExecError(cmd, args, err.Error())
				return
			}
			bucketFinished, bucketTotal := 0, 0
			for _, rb := range recv.BucketSummary {
				bucketTotal++
				if rb.Finish {
					bucketFinished++
				}
			}
			v := recv.Value
			Bucket.SetCurrent(int64(bucketFinished))
			Bucket.SetTotal(int64(bucketTotal))
			Handled.SetTotal(v.Scanned)
			Handled.SetCurrent(v.Copied + v.Failed + v.Skipped)
			Copied.SetCurrent(v.Copied)
			Skipped.SetCurrent(v.Skipped)
			Failed.SetCurrent(v.Failed)
			CopiedBytes.SetCurrent(v.Size)

			if bucketFinished == bucketTotal {
				ExecSuccess("对象同步已完成")
				return
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(StatCmd)
}
