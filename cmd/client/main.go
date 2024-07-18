package main

import (
	"context"
	"errors"
	"flag"
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

	svrIP   = flag.String("svr", "0.0.0.0", "servr IP")
	logPath = flag.String("log", "", "log path")
)

func main() {
	flag.Parse()
	logger = log.NewLogger(*logPath)

	//查询本机器IP
	localIP, err := findLocalIP()
	if err != nil {
		logger.Error().Err(err).Msg("get local IP failed.")
		return
	}

	//客户端创建链接
	conn, err := grpc.Dial(*svrIP+":50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error().Err(err).Msg("server is not connected")
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
		logger.Error().Err(err).Msg("server transport data stream error")
		return
	}

	for {
		err = stream.Send(&pb.DataRequest{Sign: "free"})
		// 接收从 服务端返回的数据流
		recv, err := stream.Recv()
		if err != nil {
			logger.Error().Err(err).Msg("server can't receive stream data")
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
				logger.Error().Err(err).Msg("put task result failed")
				return
			}
			logger.Info().Msgf("put result success, success:%v, failed:%v, deal size:%d", success, failed, dealSize)
		}
		//查询是否还有数据
		more, err := client.HasMore(context.Background(), &pb.Empty{})
		if err != nil {
			logger.Warn().Msgf("check task failed,error: %v", err)
		}
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
	return "", errors.New("findLocalIP:: network not running")
}

func doSync(task *pb.TaskInfo) (success []string, failed []string, dealSzie int64) {
	var (
		src, dst object.ObjectStorage
	)
	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	consumer := tube.NewConsumer(logger, len(task.Objects)/2)
	src, err := createStorageCache(string(task.SrcUri.Type), task.SrcUri)
	if err != nil {
		logger.Error().Msgf("dosync:: create storage failed, src:%s, err:%v", task.SrcUri, err)
		return
	}
	dst, err = createStorageCache(string(task.DestUri.Type), task.DestUri)
	if err != nil {
		logger.Error().Msgf("dosync:: create storage failed, dest:%s,err:%v", task.DestUri, err)
		return
	}

	for _, o := range task.Objects {
		wg.Add(1)
		go func(o *pb.Object) {
			defer wg.Done()
			if o.Key == "" {
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
				logger.Error().Err(err).Msgf("dosync failed, obj_name:%s, cost_time:%s, mtime:%d, size:%d", obj.Key(), time.Since(start), o.Mtime, obj.Size())
				return
			} else {
				lock.Lock()
				success = append(success, task.SrcUri.BucketDomain+"://"+o.Key)
				dealSzie += o.Size
				lock.Unlock()
				logger.Info().Msgf("dosync success, obj_name:%s, cost_time:%s, mtime:%d, size:%d", obj.Key(), time.Since(start), o.Mtime, obj.Size())
				return
			}
		}(o)
	}
	wg.Wait()
	return
}

// 缓存
func createStorageCache(storageType string, info *pb.UriInfo) (object.ObjectStorage, error) {
	key := storageType + "-" + info.BucketDomain
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
			return nil, err
		}
		storageMap.Store(key, storage)
		return storage, nil
	} else {
		return v.(object.ObjectStorage), nil
	}
}
