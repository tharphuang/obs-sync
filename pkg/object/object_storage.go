package object

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"obs-sync/models"
	"obs-sync/pkg/utils"
)

var ctx = context.Background()

var logger = utils.TubeLogger()

var UserAgent = "CucCloud"

type MtimeChanger interface {
	Chtimes(path string, mtime time.Time) error
}

type SupportSymlink interface {
	// Symlink create a symbolic link
	Symlink(oldName, newName string) error
	// Readlink read a symbolic link
	Readlink(name string) (string, error)
}

type File interface {
	Object
	Owner() string
	Group() string
	Mode() os.FileMode
}

type file struct {
	obj
	owner string
	group string
	mode  os.FileMode
}

func (f *file) Owner() string     { return f.owner }
func (f *file) Group() string     { return f.group }
func (f *file) Mode() os.FileMode { return f.mode }

func MarshalObject(o Object) map[string]interface{} {
	m := make(map[string]interface{})
	m["key"] = o.Key()
	m["size"] = o.Size()
	m["mtime"] = o.Mtime().UnixNano()
	m["isdir"] = o.IsDir()
	if f, ok := o.(File); ok {
		m["mode"] = f.Mode()
		m["owner"] = f.Owner()
		m["group"] = f.Group()
	}
	return m
}

func UnmarshalObject(m map[string]interface{}) Object {
	mtime := time.Unix(0, m["mtime"].(int64))
	o := obj{m["key"].(string), m["size"].(int64), mtime, m["isdir"].(bool)}
	if _, ok := m["mode"]; ok {
		f := file{o, m["owner"].(string), m["group"].(string), os.FileMode(m["mode"].(float64))}
		return &f
	}
	return &o
}

type FileSystem interface {
	MtimeChanger
	Chmod(path string, mode os.FileMode) error
	Chown(path string, owner, group string) error
}

var notSupported = errors.New("not supported")

type DefaultObjectStorage struct{}

func (s DefaultObjectStorage) Create() error {
	return nil
}

func (s DefaultObjectStorage) Head(key string) (Object, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) CreateMultipartUpload(key string, minSize int, _ models.CannedACLType) (*MultipartUpload, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) AbortUpload(key string, uploadID string) {}

func (s DefaultObjectStorage) CompleteUpload(key string, uploadID string, parts []*Part) error {
	return notSupported
}

func (s DefaultObjectStorage) ListUploads(marker string) ([]*PendingPart, string, error) {
	return nil, "", nil
}

func (s DefaultObjectStorage) List(prefix, marker string, limit int64) ([]Object, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) ListAll(prefix, marker string) (<-chan Object, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) GetObjectAcl(key string) (models.CannedACLType, error) {
	return "", notSupported
}

type Creator func(bucket, accessKey, secretKey string) (ObjectStorage, error)

var storages = make(map[models.ResourceType]Creator)

func Register(name models.ResourceType, register Creator) {
	storages[name] = register
}

func CreateStorage(name models.ResourceType, endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	f, ok := storages[name]
	if ok {
		return f(endpoint, accessKey, secretKey)
	}
	return nil, fmt.Errorf("invalid storage: %s", name)
}

var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 32<<10)
		return &buf
	},
}
