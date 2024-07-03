package object

import (
	"fmt"
	"io"
	"obs-sync/models"
	"os"
	"time"
)

type withPrefix struct {
	os     ObjectStorage
	prefix string
}

func (w *withPrefix) SetCheckSumKey(meta string) error {
	return notSupported
}

func (w *withPrefix) IsSetMd5(flag bool) error {
	return notSupported
}

// WithPrefix retuns an object storage that add a prefix to keys.
func WithPrefix(os ObjectStorage, prefix string) ObjectStorage {
	return &withPrefix{os, prefix}
}

func (w *withPrefix) Symlink(oldName, newName string) error {
	if w, ok := w.os.(SupportSymlink); ok {
		return w.Symlink(oldName, newName)
	}
	return notSupported
}

func (w *withPrefix) Readlink(name string) (string, error) {
	if w, ok := w.os.(SupportSymlink); ok {
		return w.Readlink(name)
	}
	return "", notSupported
}

func (w *withPrefix) String() string {
	return fmt.Sprintf("%s%s", w.os, w.prefix)
}

func (w *withPrefix) Create() error {
	return w.os.Create()
}

func (w *withPrefix) Head(key string) (Object, error) {
	o, err := w.os.Head(w.prefix + key)
	if err != nil {
		return nil, err
	}
	switch po := o.(type) {
	case *obj:
		po.key = po.key[len(w.prefix):]
	case *file:
		po.key = po.key[len(w.prefix):]
	}
	return o, nil
}

func (w *withPrefix) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return w.os.Get(w.prefix+key, off, limit)
}

func (w *withPrefix) Put(key string, in io.Reader, acl models.CannedACLType) error {
	return w.os.Put(w.prefix+key, in, acl)
}

func (w *withPrefix) Delete(key string) error {
	return w.os.Delete(w.prefix + key)
}

func (w *withPrefix) List(prefix, marker string, limit int64) ([]Object, error) {
	if marker != "" {
		marker = w.prefix + marker
	}
	objs, err := w.os.List(w.prefix+prefix, marker, limit)
	ln := len(w.prefix)
	for _, o := range objs {
		switch p := o.(type) {
		case *obj:
			p.key = p.key[ln:]
		case *file:
			p.key = p.key[ln:]
		}
	}
	return objs, err
}

func (w *withPrefix) ListAll(prefix, marker string) (<-chan Object, error) {
	if marker != "" {
		marker = w.prefix + marker
	}
	r, err := w.os.ListAll(w.prefix+prefix, marker)
	if err != nil {
		return r, err
	}
	r2 := make(chan Object, 10240)
	ln := len(w.prefix)
	go func() {
		for o := range r {
			if o != nil && o.Key() != "" {
				switch p := o.(type) {
				case *obj:
					p.key = p.key[ln:]
				case *file:
					p.key = p.key[ln:]
				}
			}
			r2 <- o
		}
		close(r2)
	}()
	return r2, nil
}

func (w *withPrefix) Chmod(path string, mode os.FileMode) error {
	if fs, ok := w.os.(FileSystem); ok {
		return fs.Chmod(w.prefix+path, mode)
	}
	return notSupported
}

func (w *withPrefix) Chown(path string, owner, group string) error {
	if fs, ok := w.os.(FileSystem); ok {
		return fs.Chown(w.prefix+path, owner, group)
	}
	return notSupported
}

func (w *withPrefix) Chtimes(key string, mtime time.Time) error {
	if fs, ok := w.os.(FileSystem); ok {
		return fs.Chtimes(w.prefix+key, mtime)
	}
	return notSupported
}

func (w *withPrefix) CreateMultipartUpload(key string, minSize int, acl models.CannedACLType) (*MultipartUpload, error) {
	return w.os.CreateMultipartUpload(w.prefix+key, minSize, acl)
}

func (w *withPrefix) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	return w.os.UploadPart(w.prefix+key, uploadID, num, body)
}

func (w *withPrefix) AbortUpload(key string, uploadID string) {
	w.os.AbortUpload(w.prefix+key, uploadID)
}

func (w *withPrefix) CompleteUpload(key string, uploadID string, parts []*Part) error {
	return w.os.CompleteUpload(w.prefix+key, uploadID, parts)
}

func (w *withPrefix) ListUploads(marker string) ([]*PendingPart, string, error) {
	parts, nextMarker, err := w.os.ListUploads(marker)
	for _, part := range parts {
		part.Key = part.Key[len(w.prefix):]
	}
	return parts, nextMarker, err
}

func (w *withPrefix) GetObjectAcl(key string) (models.CannedACLType, error) {
	acl, err := w.os.GetObjectAcl(w.prefix + key)
	return acl, err
}

var _ ObjectStorage = &withPrefix{}
