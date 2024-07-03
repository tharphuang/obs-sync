package execute

import (
	"fmt"
	"obs-sync/proto/sync/pb"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var client pb.PipeClient

var rootCmd = &cobra.Command{
	Use:   "obsync",
	Short: "obsync is a tool for object storage rsync operations",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			fmt.Println("do you forget something?")
		}
	},
}

// Execute 命令行客户端
func Execute() {
	conn, err := grpc.Dial(":50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	// 延迟关闭连接
	defer func(conn *grpc.ClientConn) {
		err = conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)
	// 初始化grpc连接
	client = pb.NewPipeClient(conn)

	rootCmd.CompletionOptions = cobra.CompletionOptions{
		DisableDefaultCmd: true,
	}
	rootCmd.Execute()
}
