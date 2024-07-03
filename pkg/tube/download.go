package tube

import (
	"errors"
	"io"
	"obs-sync/pkg/object"
	"sync"

	"github.com/juju/ratelimit"
)

type parallelDownloader struct {
	sync.Mutex
	notify     *sync.Cond
	src        object.ObjectStorage
	key        string
	fsize      int64
	blockSize  int64
	concurrent chan int
	buffers    map[int64]*Page
	off        int64
	err        error
	limiter    *ratelimit.Bucket
}

func (r *parallelDownloader) hasErr() bool {
	r.Lock()
	defer r.Unlock()
	return r.err != nil
}

func (r *parallelDownloader) setErr(err error) {
	r.Lock()
	defer r.Unlock()
	r.err = err
}

func (r *parallelDownloader) download() {
	for off := int64(0); off < r.fsize; off += r.blockSize {
		r.concurrent <- 1
		go func(off int64) {
			var size = r.blockSize
			if off+r.blockSize > r.fsize {
				size = r.fsize - off
			}
			var saved bool
			if !r.hasErr() {
				if r.limiter != nil {
					r.limiter.Wait(size)
				}
				var in io.ReadCloser
				e := try(3, func() error {
					var err error
					in, err = r.src.Get(r.key, off, size)
					return err
				})
				if e != nil {
					r.setErr(e)
				} else { //nolint:typecheck
					defer in.Close()
					p := NewOffPage(int(size))
					_, e = io.ReadFull(in, p.Data)
					if e != nil {
						r.setErr(e)
						p.Release()
					} else {
						r.Lock()
						if r.buffers != nil {
							r.buffers[off] = p
							saved = true
						} else {
							p.Release()
						}
						r.Unlock()
					}
				}
			}
			if !saved {
				<-r.concurrent
			}
			r.notify.Signal()
		}(off)
	}
}

func (r *parallelDownloader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.off >= r.fsize {
		return 0, io.EOF
	}
	off := r.off / r.blockSize * r.blockSize
	r.Lock()
	for r.err == nil && r.buffers[off] == nil {
		r.notify.Wait()
	}
	p := r.buffers[off]
	r.Unlock()
	if p == nil {
		return 0, r.err
	}
	n := copy(b, p.Data[r.off-off:])
	r.off += int64(n)
	if r.off == off+int64(len(p.Data)) {
		p.Release()
		r.Lock()
		delete(r.buffers, off)
		r.Unlock()
		<-r.concurrent
	}
	return n, nil
}

func (r *parallelDownloader) Close() {
	r.Lock()
	defer r.Unlock()
	for _, p := range r.buffers {
		p.Release()
	}
	r.buffers = nil
	if r.err == nil {
		r.err = errors.New("closed")
	}
}

func newParallelDownloader(store object.ObjectStorage, key string, size int64, bSize int64, concurrent chan int, limiter *ratelimit.Bucket) *parallelDownloader {
	down := &parallelDownloader{
		src:        store,
		key:        key,
		fsize:      size,
		blockSize:  bSize,
		concurrent: concurrent,
		buffers:    make(map[int64]*Page),
		limiter:    limiter,
	}
	down.notify = sync.NewCond(down)
	go down.download()
	return down
}
