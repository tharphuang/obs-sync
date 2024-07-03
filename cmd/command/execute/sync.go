package execute

import (
	"context"
	"errors"
	"fmt"
	"obs-sync/models"
	"obs-sync/proto/sync/pb"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

// 提交迁移任务 {ak}:{sk}@s3://region
var submitCmd = &cobra.Command{
	Use:   "obsync",
	Short: "obsync task config file.",
	Long:  "obsync task form the task config file.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			unpackGrpcError(cmd, args, errors.New("invalid args, expected two arguments, source uri and dest uri"))
			return
		}
		srcUri, err := parseUri(args[0])
		if err != nil {
			ExecError(cmd, args, err.Error())
			return
		}
		destUri, err := parseUri(args[1])
		if err != nil {
			ExecError(cmd, args, err.Error())
			return
		}

		res, err := client.Sync(context.Background(), &pb.SyncInfo{
			Src: &pb.Auth{
				Type:      string(srcUri.Type),
				AccessKey: srcUri.AccessKey,
				SecretKey: srcUri.SecretKey,
				Region:    srcUri.Region,
			},
			Dest: &pb.Auth{
				Type:      string(destUri.Type),
				AccessKey: destUri.AccessKey,
				SecretKey: destUri.SecretKey,
				Region:    destUri.Region,
			},
		})
		if err != nil {
			ExecError(cmd, args, err.Error())
		}

		fmt.Println("==>添加任务成功，任务信息如下：")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"源端类型", "源端bucket域名", "同步方向", "目的端类型", "目的端bucket域名"})
		table.SetBorder(true)
		table.SetColumnColor(
			tablewriter.Colors{},
			tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlueColor},
			tablewriter.Colors{tablewriter.Bold, tablewriter.FgGreenColor},
			tablewriter.Colors{},
			tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlueColor},
		)

		for _, r := range res.Buckets {
			table.Append([]string{string(srcUri.Type), r.Cells[0], r.Cells[1], string(destUri.Type), r.Cells[2]})
		}
		table.Render()
	},
}

func parseUri(uriStr string) (*models.Uri, error) {
	var (
		uri        models.Uri
		authInfo   string
		regionInfo string
	)
	if strings.Contains(uriStr, "@") {
		parts := strings.Split(uriStr, "@")
		authInfo, regionInfo = parts[0], parts[1]
	} else {
		return nil, errors.New("uri must be a valid value, like: s3:ak@sk")
	}
	if strings.Contains(authInfo, ":") {
		parts := strings.Split(authInfo, ":")
		uri.AccessKey, uri.SecretKey = parts[0], parts[1]
	} else {
		return nil, errors.New("uri must be a valid value, like: s3:ak@sk")
	}
	if strings.Contains(regionInfo, "://") {
		parts := strings.Split(regionInfo, "://")
		uri.Type, uri.Region = models.ResourceType(parts[0]), parts[1]
	}
	return &uri, nil
}

func init() {
	rootCmd.AddCommand(submitCmd)
}
