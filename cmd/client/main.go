package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"obs-sync/infra/log"
	"obs-sync/models"
	"obs-sync/pkg/cloudstorage"
	"obs-sync/pkg/object"
	"obs-sync/pkg/tube"
	"obs-sync/proto/sync/pb"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	storageMap sync.Map
	logger     *log.Logger

	svrIP   = flag.String("svr", "0.0.0.0", "servrIP")
	logPath = flag.String("log", "", "log path")
)

func main() {
	flag.Parse()
	logger = log.NewLogger(*logPath)

	//查询本机器IP
	localIP, err := findLocalIP()
	if err != nil {
		logger.Error().Err(err).Msg("本机器IP查询失败,迁移节点client部署失败，请检查你的网络环境重新部署")
		fmt.Println("error: 本机器IP查询有误，部署失败")
		return
	}

	//客户端创建链接
	conn, err := grpc.Dial(*svrIP+":50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error().Err(err).Msg("server端连接创建失败")
		fmt.Println("error: 迁移节点client无法连接到server端")
		return
	}
	// 延迟关闭连接
	defer func(conn *grpc.ClientConn) {
		err = conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)

	client := pb.NewPipeClient(conn)
	ctx := context.Background()
	stream, err := client.DataStream(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("创建server数据流失败")
		fmt.Println("error: 迁移节点client无法连接到server端")
		return
	}

	for {
		err = stream.Send(&pb.DataRequest{Sign: "free"})
		// 接收从 服务端返回的数据流
		recv, err := stream.Recv()
		if err != nil {
			logger.Error().Err(err).Msg("server数据流接收完成")
			break
		}
		logger.Info().Msgf("received task: %v", recv.Task)
		success, failed, dealSize := doSync(recv.Task)
		if len(success) != 0 || len(failed) != 0 {
			_, err = client.PutResult(context.Background(), &pb.Result{
				BucketName: recv.Task.BucketName,
				WorkIP:     localIP,
				Success:    success,
				Failed:     failed,
				DeadlSize:  dealSize,
			})
			if err != nil {
				logger.Error().Err(err).Msg("上传处理数据到管理节点失败")
				return
			}
			logger.Info().Msgf("put result success, success:%v, failed:%v, dealSize:%d", success, failed, dealSize)
		}
		//查询是否还有数据
		more, _ := client.HasMore(context.Background(), &pb.Empty{})
		if !more.Has {
			time.Sleep(10 * time.Second)
		}
	}
}

func findLocalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("请检查设备是否联网")
}

func doSync(task *pb.TaskInfo) (success []string, failed []string, dealSzie int64) {
	var (
		src, dst object.ObjectStorage
	)

	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	consumer := tube.NewConsumer(logger, len(task.Objects)/2)

	src = createStorageCache(string(task.SrcUri.Type), task.SrcUri)
	dst = createStorageCache(string(task.DestUri.Type), task.DestUri)

	for _, o := range task.Objects {
		wg.Add(1)
		go func(o *pb.Object) {
			defer wg.Done()
			if o.Key == "" {
				return
			}
			//dst.IsSetMd5(true)
			if src == nil || dst == nil {
				//failed = append(failed, &pipe.JobStatus{Id: "", Key: o.Key, Size: o.Size})
				logger.Error().Msgf("对象构建失败,源:%s, 目的:%v", task.SrcUri, task.DestUri)
				return
			}

			var acl models.CannedACLType
			acl, _ = src.GetObjectAcl(o.Key)

			start := time.Now()
			obj := object.UnmarshalObject(map[string]interface{}{
				"key":   o.Key,
				"mtime": o.Mtime,
				"isdir": o.IsDir,
				"size":  o.Size,
			})
			err := consumer.Work(src, dst, obj, acl)
			logger.Info().Msgf("%v, success", obj)
			if err != nil {
				lock.Lock()
				failed = append(failed, task.SrcUri.BucketDomain+"://"+o.Key)
				dealSzie += o.Size
				lock.Unlock()
				logger.Error().Err(err).Msgf("对象迁移失败: obj_name:%s, cost_time:%s, mtime:%d, size:%d", obj.Key(), time.Since(start), o.Mtime, obj.Size())
				return
			} else {
				lock.Lock()
				success = append(success, task.SrcUri.BucketDomain+"://"+o.Key)
				dealSzie += o.Size
				lock.Unlock()
				logger.Info().Msgf("对象迁移成功: obj_name:%s, cost_time:%s, mtime:%d, size:%d", obj.Key(), time.Since(start), o.Mtime, obj.Size())
				return
			}
		}(o)
		// 迁移配置设置
	}
	wg.Wait()
	return
}

// 缓存
func createStorageCache(t string, info *pb.UriInfo) object.ObjectStorage {
	key := t + "-" + info.BucketDomain
	if v, ok := storageMap.Load(key); !ok {
		infoModels := models.UriInfo{
			Type:         models.ResourceType(info.Type),
			Scheme:       info.Scheme,
			BucketDomain: info.BucketDomain,
			AccessKey:    info.AccessKey,
			SecretKey:    info.SecretKey,
		}
		storage, err := cloudstorage.CreateStorage(infoModels)
		if err != nil {
			storageMap.Store(key, storage)
		}
		return storage
	} else {
		return v.(object.ObjectStorage)
	}
}
