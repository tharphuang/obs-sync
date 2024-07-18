package main

import (
	"flag"
	"fmt"
	"net"
	"obs-sync/cmd/server/service"
	"obs-sync/proto/sync/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	logPath = flag.String("log", "", "log path")
)

func main() {
	listen, err := net.Listen("tcp", ":50051")
	if err != nil {
		fmt.Printf("server listen failed, err:%s\n", err.Error())
		return
	}

	// grpc服务端日志
	server := grpc.NewServer()
	pipeService := service.NewServer(*logPath)
	pb.RegisterPipeServer(server, pipeService)
	reflection.Register(server)
	if err = server.Serve(listen); err != nil {
		fmt.Printf("server start failed, err:%s\n", err.Error())
	}
}
