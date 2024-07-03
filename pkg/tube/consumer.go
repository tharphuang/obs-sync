package tube

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"obs-sync/infra/log"
	"obs-sync/models"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/juju/ratelimit"

	"obs-sync/pkg/object"
)

const (
	defaultPartSize = 50 << 20
	defaultThread   = 10
)

var (
	maxBlock    = int64(defaultPartSize * 2)
	maxPartSize = defaultPartSize
	l           *log.Logger
)

var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 32<<10)
		return &buf
	},
}

type Consumer struct {
	concurrent chan int          //原子限制器，防止重复复制
	limiter    *ratelimit.Bucket // 限速开关
}

func NewConsumer(mylog *log.Logger, threads int) *Consumer {
	l = mylog
	// 初始化x系统并发线程数量
	if threads <= 0 {
		threads = defaultThread
	}
	concurrent := make(chan int, threads)
	return &Consumer{
		concurrent: concurrent,
	}
}

func try(n int, f func() error) (err error) {
	for i := 0; i < n; i++ {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(time.Second * time.Duration(i*i))
	}
	return
}

// OpenLimiter 对外开发接口,控制c端下载源端数据的速度
func (c *Consumer) OpenLimiter(limit int) {
	if limit > 0 {
		mbps := float64(limit * (1 << 20)) // 10% overhead
		c.limiter = ratelimit.NewBucketWithRate(mbps, int64(mbps)*3)
	} else {
		if c.limiter != nil {
			c.limiter = nil
		}
	}
}

func (c *Consumer) Work(src, dst object.ObjectStorage, obj object.Object, acl models.CannedACLType) error {
	var err error
	if obj.Size() < maxBlock {
		err = try(3, func() error { return c.doCopySingle(src, dst, obj, acl) })
	} else {
		var upload *object.MultipartUpload
		var dstKey string
		if strings.HasPrefix(src.String(), "url://") {
			keyArray := strings.Fields(obj.Key())
			if len(keyArray) != 2 {
				return errors.New("url object key not valid")
			}
			dstKey = keyArray[1]
		} else {
			dstKey = obj.Key()
		}
		if upload, err = dst.CreateMultipartUpload(dstKey, defaultPartSize, acl); err == nil {
			err = c.doCopyMultiple(src, dst, obj, upload)
		} else { // fallback
			err = try(3, func() error { return c.doCopySingle(src, dst, obj, acl) })
		}
	}
	return err
}

func (c *Consumer) doCopySingle(src, dst object.ObjectStorage, obj object.Object, acl models.CannedACLType) error {
	if obj.Size() > maxBlock || !(strings.HasPrefix(src.String(), "file://") || strings.HasPrefix(src.String(), "url://")) {
		var err error
		var in io.Reader
		downer := newParallelDownloader(src, obj.Key(), obj.Size(), int64(maxPartSize), c.concurrent, c.limiter)
		defer downer.Close()
		if strings.HasPrefix(dst.String(), "file://") {
			in = downer
		} else {
			var f *os.File
			// download the object into disk
			if f, err = ioutil.TempFile("", "rep"); err != nil {
				l.Warn().Msgf("create temp file: %s", err)
				goto SINGLE
			}
			_ = os.Remove(f.Name()) // will be deleted after Close()
			defer f.Close()
			buf := bufPool.Get().(*[]byte)
			defer bufPool.Put(buf)
			if _, err = io.CopyBuffer(struct{ io.Writer }{f}, downer, *buf); err == nil {
				_, err = f.Seek(0, 0)
				in = f
			}
		}
		if err == nil {
			err = dst.Put(obj.Key(), in, acl)
		}
		if err != nil {
			if _, e := src.Head(obj.Key()); os.IsNotExist(e) {
				l.Log().Msgf("Head src %s: %s", obj.Key(), err)
				err = nil
			}
		}
		return err
	}
SINGLE:
	if c.limiter != nil {
		c.limiter.Wait(obj.Size())
	}
	c.concurrent <- 1
	defer func() {
		<-c.concurrent
	}()
	var in io.ReadCloser
	var err error
	if obj.Size() == 0 {
		in = io.NopCloser(bytes.NewReader(nil))
	} else {
		in, err = src.Get(obj.Key(), 0, -1)
		if err != nil {
			if _, e := src.Head(obj.Key()); os.IsNotExist(e) {
				l.Debug().Msgf("Head src %s: %s", obj.Key(), err)
				err = nil
			}
			return err
		}
	}
	defer in.Close()
	if strings.HasPrefix(src.String(), "url://") {
		keyArray := strings.Fields(obj.Key())
		if len(keyArray) != 2 {
			return errors.New("url object key not valid")
		}
		return dst.Put(keyArray[1], in, acl)
	}
	return dst.Put(obj.Key(), in, acl)
}

func (c *Consumer) doCopyMultiple(src, dst object.ObjectStorage, obj object.Object, upload *object.MultipartUpload) error {
	var objKey string
	if strings.HasPrefix(src.String(), "url://") {
		keyArray := strings.Fields(obj.Key())
		if len(keyArray) != 2 {
			return errors.New("url object key not valid")
		}
		objKey = keyArray[1]
	} else {
		objKey = obj.Key()
	}
	partSize := int64(upload.MinPartSize)
	if partSize == 0 {
		partSize = defaultPartSize
	}
	if obj.Size() > partSize*int64(upload.MaxCount) {
		partSize = obj.Size() / int64(upload.MaxCount)
		partSize = ((partSize-1)>>20 + 1) << 20 // align to MB
	}
	n := int((obj.Size()-1)/partSize) + 1
	l.Info().Msgf("Copying data of %s as %d parts (size: %d,max count of block:%d),uploadID: %s", objKey, n, partSize, upload.MaxCount, upload.UploadID)
	abort := make(chan struct{})
	parts := make([]*object.Part, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(num int) {
			sz := partSize
			if num == n-1 {
				sz = obj.Size() - int64(num)*partSize
			}
			if c.limiter != nil {
				c.limiter.Wait(sz)
			}
			select {
			case <-abort:
				errs <- fmt.Errorf("aborted")
				return
			case c.concurrent <- 1:
				defer func() {
					<-c.concurrent
				}()
			}

			data := make([]byte, sz)
			if err := try(3, func() error {
				in, err := src.Get(obj.Key(), int64(num)*partSize, sz)
				if err != nil {
					return err
				}
				defer in.Close()
				if _, err = io.ReadFull(in, data); err != nil {
					return err
				}
				// PartNumber starts from 1
				parts[num], err = dst.UploadPart(objKey, upload.UploadID, num+1, data)
				return err
			}); err == nil {
				errs <- nil
				l.Info().Msgf("Copied data of %s part %d", obj.Key(), num)
			} else {
				errs <- fmt.Errorf("part %d: %s", num, err)
				l.Error().Err(err).Msgf("Copy data of %s part %d failed", obj.Key(), num)
			}
			return
		}(i)
	}

	var err error
	for i := 0; i < n; i++ {
		if err = <-errs; err != nil {
			close(abort)
			break
		}
	}
	if err == nil {
		err = try(3, func() error { return dst.CompleteUpload(objKey, upload.UploadID, parts) })
	}
	if err != nil {
		dst.AbortUpload(objKey, upload.UploadID)
		return fmt.Errorf("multipart: %s", err)
	}
	return nil
}
