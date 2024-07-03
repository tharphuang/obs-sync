package execute

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/status"
)

func ExecError(cmd *cobra.Command, args []string, msg string) {
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "execute: %s %s\n \033[1;31;40m error:%s\033[0m\n", cmd.Name(), args, msg)
	} else {
		fmt.Fprintf(os.Stderr, "execute: %s \n \033[1;31;40m error:%s\033[0m\n", cmd.Name(), msg)
	}
	os.Exit(1)
}

func unpackGrpcError(cmd *cobra.Command, args []string, err error) {
	fromError, _ := status.FromError(err)

	if fromError.Code() == 1 || fromError.Code() == 2 {
		ExecError(cmd, args, fromError.Message())
		return
	}
	if fromError.Code() == 14 {
		ExecError(cmd, args, "迁移服务进程或网络不可用,请重新部署server端服务")
		return
	}
	ExecError(cmd, args, "未知错误类型")
}

func ExecSuccess(msg string) {
	fmt.Fprintf(os.Stderr, "\033[1;32;40m success:%s \033[0m\n", msg)
}
