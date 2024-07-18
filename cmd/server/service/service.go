package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"obs-sync/infra/log"
	"obs-sync/models"
	"obs-sync/pkg/bucket"
	"obs-sync/pkg/cloudstorage"
	"obs-sync/pkg/object"
	"obs-sync/proto/sync/pb"
	"sync"
	"time"
)

const batchNumber = 500

var (
	SyncInfo  = &models.SyncInfo{}
	Stats     = sync.Map{}
	IsRunning bool
	TaskChan  = make(chan models.Task, 1024)
	l         *log.Logger
)

type server struct{}

// Stat implements pb.PipeServer.
func (s *server) Stat(_ *pb.Empty, send pb.Pipe_StatServer) error {
	l.Info().Msgf("stat")
	ctx := send.Context()
	for {
		select {
		case <-ctx.Done():
			l.Info().Msg("stat streaming has been discontinue")
			return nil
		default:
			value := pb.Value{}
			buckets := []*pb.BucketSummary{}
			Stats.Range(func(k, v any) bool {
				stat := v.(models.Stats)
				value.Scanned += stat.Scanned
				value.Copied += stat.Copied
				value.Failed += stat.Failed
				value.Size += stat.Size
				value.Skipped += stat.Skipped
				buckets = append(buckets, &pb.BucketSummary{
					Name:    k.(string),
					Scan:    stat.Scanned,
					Success: stat.Copied,
					Fail:    stat.Failed,
					Finish:  stat.FinishFlag,
				})
				return true
			})
			err := send.Send(&pb.StatResult{
				Value:         &value,
				BucketSummary: buckets,
			})
			if err != nil {
				l.Error().Err(err)
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// DataStream implements pb.PipeServer.
func (s *server) DataStream(stream pb.Pipe_DataStreamServer) error {
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			l.Info().Msg("DataStream:: client context closed signal.")
			return ctx.Err()
		default:
			// 接收从客户端发来的消息 输入,
			recv, err := stream.Recv()
			if err == io.EOF {
				l.Info().Msg("DataStream:: client io eof.")
				return nil
			}
			if err != nil {
				l.Error().Err(err).Msg("DataStream:: client unavailable.")
				return err
			}
			// 如果接收正常，则根据接收到的 字符串 执行相应的指令
			switch recv.Sign {
			case "close":
				l.Info().Msg("DataStream:: receive close signal")
				if err := stream.Send(&pb.DataResponse{Task: nil}); err != nil {
					return err
				}
				return nil
			case "free": //空闲指令
				task := <-TaskChan
				var objs []*pb.Object
				for _, o := range task.Objs {
					objs = append(objs, &pb.Object{
						Key:   o.Key,
						Size:  o.Size,
						Mtime: o.Mtime,
						IsDir: o.IsDir,
					})
				}
				if err = stream.Send(&pb.DataResponse{Task: &pb.TaskInfo{
					BucketName: task.BuckeNmae,
					SrcUri: &pb.UriInfo{
						Type:         string(task.SrcInfo.Type),
						Scheme:       task.SrcInfo.Scheme,
						BucketDomain: task.SrcInfo.BucketDomain,
						AccessKey:    task.SrcInfo.AccessKey,
						SecretKey:    task.SrcInfo.SecretKey,
					},
					DestUri: &pb.UriInfo{
						Type:         string(task.DestInfo.Type),
						Scheme:       task.DestInfo.Scheme,
						BucketDomain: task.DestInfo.BucketDomain,
						AccessKey:    task.DestInfo.AccessKey,
						SecretKey:    task.DestInfo.SecretKey,
					},
					Objects: objs,
				}}); err != nil {
					l.Error().Err(err).Msg("发送对象列表失败")
					return err
				}
				l.Info().Msgf("send task success, %v", task)
			default:
				// 缺省情况下， 返回 '服务端返回: ' + 输入信息
				l.Info().Msgf("[收到消息]: %s", recv.Sign)
				if err = stream.Send(&pb.DataResponse{Task: nil}); err != nil {
					return err
				}
			}
		}
	}
}

// HasMore implements pb.PipeServer.
func (s *server) HasMore(context.Context, *pb.Empty) (*pb.HasMoreReplay, error) {
	return &pb.HasMoreReplay{
		Has: !(len(TaskChan) == 0),
	}, nil
}

// PutResult implements pb.PipeServer.
func (s *server) PutResult(ctx context.Context, r *pb.Result) (*pb.Replay, error) {
	l.Info().Msgf("put result: request: %v", r)
	if v, ok := Stats.Load(r.BucketName); ok {
		newVule := v.(models.Stats)
		newVule.Copied += int64(len(r.Success))
		newVule.Failed += int64(len(r.Failed))
		newVule.Size += r.DeadlSize
		if newVule.Copied+newVule.Failed == newVule.Scanned {
			newVule.FinishFlag = true
			l.Info().Msgf("put result: bucket:%s sync finished.", r.BucketName)
		}
		Stats.Store(r.BucketName, newVule)
	} else {
		return &pb.Replay{Status: "-1"}, errors.New("bucket stat not found")
	}
	l.Info().Msgf("put result: worker:%s success:%v failed:%v", r.WorkIP, r.Success, r.Failed)
	return &pb.Replay{Status: "0"}, nil
}

// Start implements pb.PipeServer.
func (s *server) Start(_ *pb.Empty, send pb.Pipe_StartServer) error {
	l.Info().Msgf("statrt")
	ctx := send.Context()
	if !IsRunning {
		for _, r := range SyncInfo.BucketRanks {
			switch r.Orientation {
			case models.To:
				err := bucket.BucketStorage(SyncInfo.DestUri.Type, SyncInfo.DestUri.AccessKey, SyncInfo.DestUri.SecretKey).Create(SyncInfo.DestUri.Region, r.Name)
				if err != nil {
					l.Error().Msgf("error creating info: %v ,bucket: %s ,err: %v", SyncInfo.DestUri, r.Name, err)
					continue
				}
				go listAllObj(SyncInfo.SrcUri, SyncInfo.DestUri, r)
			case models.From:
				err := bucket.BucketStorage(SyncInfo.SrcUri.Type, SyncInfo.SrcUri.AccessKey, SyncInfo.SrcUri.SecretKey).Create(SyncInfo.SrcUri.Region, r.Name)
				if err != nil {
					l.Error().Msgf("error creating info: %v ,bucket: %s ,err: %v", SyncInfo.SrcUri, r.Name, err)
					continue
				}
				go listAllObj(SyncInfo.DestUri, SyncInfo.SrcUri, r)
			case models.With:
				go syncObj(r)
			}
		}
		IsRunning = true
	} else {
		return errors.New("sync task is running, you can use stat to check")
	}

	for {
		select {
		case <-ctx.Done():
			l.Info().Msg("start streaming has been discontinue")
			return nil
		default:
			value := pb.Value{}
			Stats.Range(func(k, v any) bool {
				stats := v.(models.Stats)
				value.Scanned += stats.Scanned
				value.Copied += stats.Copied
				value.Failed += stats.Failed
				value.Size += stats.Size
				value.Skipped += stats.Skipped
				return true
			})
			err := send.Send(&pb.Status{Value: &value})
			if err != nil {
				l.Error().Err(err)
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// Stop implements pb.PipeServer.
func (s *server) Stop(context.Context, *pb.Empty) (*pb.StopResult, error) {
	return nil, errors.New("unimplemented")
}

// Sync implements pb.PipeServer.
func (s *server) Sync(ctx context.Context, r *pb.SyncInfo) (*pb.SyncReplay, error) {
	l.Info().Msgf("sync: request: %v", r)
	srcBuckets, err := bucket.BucketStorage(models.ResourceType(r.Src.Type), r.Src.AccessKey, r.Src.SecretKey).List(r.Src.Region)
	if err != nil {
		l.Error().Msgf("sync: failed to list bucket, info:%v, error: %v", r.Src, err)
		return nil, err
	}
	destBuckets, err := bucket.BucketStorage(models.ResourceType(r.Dest.Type), r.Dest.AccessKey, r.Dest.SecretKey).List(r.Dest.Region)
	if err != nil {
		l.Error().Msgf("sync: failed to list bucket, info:%v, error: %v", r.Dest, err)
		return nil, err
	}

	ranks := rankBuckets(srcBuckets, destBuckets, models.ResourceType(r.Src.Type), models.ResourceType(r.Dest.Type), r.Src.Region, r.Dest.Region)
	var buckets []*pb.SyncReplay_Row
	for _, rank := range ranks {
		buckets = append(buckets, &pb.SyncReplay_Row{
			Cells: []string{rank.SrcBucket, rank.Ori(), rank.DestBucket},
		})
	}
	SyncInfo = &models.SyncInfo{
		SrcUri:      models.Uri{Type: models.ResourceType(r.Src.Type), AccessKey: r.Src.AccessKey, SecretKey: r.Src.SecretKey, Region: r.Src.Region},
		DestUri:     models.Uri{Type: models.ResourceType(r.Dest.Type), AccessKey: r.Dest.AccessKey, SecretKey: r.Dest.SecretKey, Region: r.Dest.Region},
		BucketRanks: ranks,
	}
	l.Info().Msgf("sync: success, ranked buckets:%v ", ranks)
	return &pb.SyncReplay{
		Status:  "0",
		Buckets: buckets,
	}, nil
}

func NewServer(path string) pb.PipeServer {
	l = log.NewLogger(path).SetLevel("INFO")
	return &server{}
}

func listAllObj(s, d models.Uri, ori models.BucketOri) error {
	info := models.UriInfo{
		Type:         s.Type,
		Scheme:       "http",
		BucketDomain: ori.SrcBucket,
		AccessKey:    s.AccessKey,
		SecretKey:    s.SecretKey,
	}
	destInfo := models.UriInfo{
		Type:         d.Type,
		Scheme:       "http",
		BucketDomain: ori.DestBucket,
		AccessKey:    d.AccessKey,
		SecretKey:    d.SecretKey,
	}
	storage, err := cloudstorage.CreateStorage(info)
	if err != nil {
		l.Error().Msgf("list all obj create info:%v, error:%v \n", info, err)
		return err
	}
	ch, err := listAll(storage, "", "")
	if err != nil {
		l.Error().Msgf("list all obj create info:%v, error:%v \n", info, err)
		return err
	}

	var (
		task models.Task
		objs []models.Obj
	)
	for o := range ch {
		objs = append(objs, models.Obj{
			Key:   o.Key(),
			Size:  o.Size(),
			Mtime: o.Mtime().Unix(),
			IsDir: o.IsDir(),
		})
		if len(objs) == batchNumber {
			task = models.Task{
				BuckeNmae: ori.Name,
				SrcInfo:   info,
				DestInfo:  destInfo,
				Objs:      objs,
			}
			TaskChan <- task
			updateStatsScaned(ori.Name, len(objs))
			l.Info().Msgf("list all and send to channel success, task:%v", task)
			objs = []models.Obj{}
		}
	}
	if len(objs) > 0 {
		task = models.Task{
			BuckeNmae: ori.Name,
			SrcInfo:   info,
			DestInfo:  destInfo,
			Objs:      objs,
		}
		TaskChan <- task
		updateStatsScaned(ori.Name, len(objs))
		l.Info().Msgf("list all and send to channel success, task:%v", task)
	}
	return nil
}

func updateStatsScaned(bucket string, scaned int) {
	if v, ok := Stats.Load(bucket); ok {
		tmpValue := v.(models.Stats)
		tmpValue.Scanned += int64(scaned)
		Stats.Store(bucket, tmpValue)
	} else {
		Stats.Store(bucket, models.Stats{Scanned: int64(scaned)})
	}
}

func syncObj(ori models.BucketOri) error {
	srcInfo := models.UriInfo{
		Type:         SyncInfo.SrcUri.Type,
		Scheme:       "http",
		BucketDomain: ori.SrcBucket,
		AccessKey:    SyncInfo.SrcUri.AccessKey,
		SecretKey:    SyncInfo.SrcUri.SecretKey,
	}
	src, err := cloudstorage.CreateStorage(srcInfo)
	if err != nil {
		l.Error().Msgf("sync obj create info:%v, error:%v", srcInfo, err)
		return nil
	}
	srcKeysChan, err := listAll(src, "", "")
	if err != nil {
		l.Error().Msgf("sync obj listAll info:%v, error:%v", srcInfo, err)
		return err
	}

	destInfo := models.UriInfo{
		Type:         SyncInfo.DestUri.Type,
		Scheme:       "http",
		BucketDomain: ori.DestBucket,
		AccessKey:    SyncInfo.DestUri.AccessKey,
		SecretKey:    SyncInfo.DestUri.SecretKey,
	}
	dest, err := cloudstorage.CreateStorage(destInfo)
	if err != nil {
		l.Error().Msgf("sync obj create info:%v, error:%v", destInfo, err)
		return nil
	}
	dstKeysChan, err := listAll(dest, "", "")
	if err != nil {
		l.Error().Msgf("sync obj listAll info:%v, error:%v", destInfo, err)
		return err
	}

	var (
		dObj     object.Object
		srcObjs  []models.Obj
		destObjs []models.Obj
	)

	for obj := range srcKeysChan {
		objKey := obj.Key()
		if dObj != nil && objKey > dObj.Key() {
			dObj = nil
		}
		if dObj == nil {
			for dObj = range dstKeysChan {
				if dObj == nil {
					return nil
				}
				if objKey <= dObj.Key() {
					break
				}
				// TODO: 确定是否需要删除
				dObj = nil
			}
		}

		if dObj == nil || objKey < dObj.Key() {
			srcObjs = append(srcObjs, models.Obj{
				Key:   obj.Key(),
				IsDir: obj.IsDir(),
				Mtime: obj.Mtime().Unix(),
				Size:  obj.Size(),
			})
		} else if objKey == dObj.Key() {
			l.Info().Msgf("skip %s", objKey)
		}

		if len(srcObjs) == batchNumber {
			task := models.Task{
				BuckeNmae: ori.Name,
				SrcInfo:   srcInfo,
				DestInfo:  destInfo,
				Objs:      srcObjs,
			}
			TaskChan <- task
			updateStatsScaned(ori.Name, len(srcObjs))
			srcObjs = nil
			l.Debug().Msgf("send to channel success, task:%v", task)
		}
		if len(destObjs) == batchNumber {
			task := models.Task{
				BuckeNmae: ori.Name,
				SrcInfo:   destInfo,
				DestInfo:  srcInfo,
				Objs:      destObjs,
			}
			TaskChan <- task
			updateStatsScaned(ori.Name, len(destObjs))
			destObjs = nil
			l.Debug().Msgf("send to channel success, task:%v", task)
		}
	}
	if len(srcObjs) > 0 {
		task := models.Task{
			BuckeNmae: ori.Name,
			SrcInfo:   srcInfo,
			DestInfo:  destInfo,
			Objs:      srcObjs,
		}
		TaskChan <- task
		updateStatsScaned(ori.Name, len(srcObjs))
		l.Debug().Msgf("send to channel success, task:%v", task)
	}

	if len(destObjs) > 0 {
		task := models.Task{
			BuckeNmae: ori.Name,
			SrcInfo:   destInfo,
			DestInfo:  srcInfo,
			Objs:      destObjs,
		}
		TaskChan <- task
		updateStatsScaned(ori.Name, len(destObjs))
		l.Debug().Msgf("send to channel success, task:%v", task)
	}
	return err
}

func listAll(store object.ObjectStorage, start, end string) (<-chan object.Object, error) {
	var maxResults int64 = 1000
	out := make(chan object.Object, maxResults)
	// 列举起始标记
	if start != "" {
		if obj, err := store.Head(start); err == nil {
			out <- obj
		}
	}

	// 支持全量列举
	if ch, err := store.ListAll("", start); err == nil {
		if end == "" {
			go func() {
				for obj := range ch {
					out <- obj
				}
				close(out)
			}()
			return out, nil
		}

		go func() {
			for obj := range ch {
				if obj != nil && obj.Key() > end {
					break
				}
				out <- obj
			}
			close(out)
		}()
		return out, nil
	}

	//不支持全量迁移，需要从标记位开始迁。
	marker := start
	objs, err := store.List("", marker, maxResults)
	if err != nil {
		return nil, err
	}
	go func() {
		lastkey := ""
		first := true
	END:
		for len(objs) > 0 {
			for _, obj := range objs {
				key := obj.Key()
				if !first && key <= lastkey {
					l.Info().Msgf("listAll: the keys are out of order: marker %q, last %q current %q", marker, lastkey, key)
				}
				if end != "" && key > end {
					break END
				}
				lastkey = key
				out <- obj
				first = false
			}
			// Corner case: the func parameter `marker` is an empty string("") and exactly
			// one object which key is an empty string("") returned by the List() method.
			if lastkey == "" {
				break END
			}

			marker = lastkey
			objs, err = store.List("", marker, maxResults)
			count := 1
			for err != nil && count <= 3 {
				// slow down
				time.Sleep(time.Millisecond * time.Duration(100*count))
				objs, err = store.List("", marker, maxResults)
				count++
			}
			if err != nil {
				// Telling that the listing has failed
				out <- nil
				break
			}
			if len(objs) > 0 && objs[0].Key() == marker {
				// workaround from a object store that is not compatible to S3.
				objs = objs[1:]
			}
		}
		close(out)
	}()
	return out, nil
}

func rankBuckets(src, dest []bucket.BucketInfo, sType, dType models.ResourceType, sRegion, dRegion string) (res []models.BucketOri) {
	if len(src) == 0 {
		for _, d := range dest {
			domain := coverBucketDomain(sType, d.Name, sRegion)
			res = append(res, models.BucketOri{SrcBucket: domain, Orientation: models.From, DestBucket: d.Domain, Name: d.Name})
		}
		return
	}
	if len(dest) == 0 {
		for _, s := range dest {
			domain := coverBucketDomain(dType, s.Name, dRegion)
			res = append(res, models.BucketOri{SrcBucket: s.Domain, Orientation: models.To, DestBucket: domain, Name: s.Name})
		}
		return
	}

	mapSrc := make(map[string]bool)
	for _, v := range src {
		mapSrc[v.Name] = true
	}
	for _, d := range dest {
		domain := coverBucketDomain(sType, d.Name, sRegion)
		if mapSrc[d.Name] {
			res = append(res, models.BucketOri{SrcBucket: domain, Orientation: models.With, DestBucket: d.Domain, Name: d.Name})
			mapSrc[d.Name] = false
		} else {
			res = append(res, models.BucketOri{SrcBucket: domain, Orientation: models.From, DestBucket: d.Domain, Name: d.Name})
		}
	}
	for n, v := range mapSrc {
		if v {
			s := coverBucketDomain(sType, n, sRegion)
			d := coverBucketDomain(dType, n, dRegion)
			res = append(res, models.BucketOri{SrcBucket: s, Orientation: models.To, DestBucket: d, Name: n})
		}
	}
	return res
}

func coverBucketDomain(t models.ResourceType, name string, region string) string {
	switch t {
	case models.Cos:
		return fmt.Sprintf("%s.cos.%s.myqcloud.com", name, region)
	case models.Cuc:
		return fmt.Sprintf("%s.obs-%s-internal.cucloud.cn", name, region)
	case models.Obs:
		return fmt.Sprintf("%s.obs.%s.myhuaweicloud.com", name, region)
	case models.Oss:
		return fmt.Sprintf("%s.%s.aliyuncs.com", name, region)
	case models.S3:
		return fmt.Sprintf("%s.%s.amazonaws.com", name, region)
	default:
		return ""
	}
}
