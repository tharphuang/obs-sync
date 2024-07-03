package object

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"obs-sync/models"
	"os"
	"path"
	"runtime"
	"strings"
)

type urlStorage struct {
	DefaultObjectStorage
	bucket string
	path   string
}

func (u *urlStorage) SetCheckSumKey(meta string) error {
	return notSupported
}

func (u *urlStorage) IsSetMd5(flag bool) error {
	return notSupported
}

func (u *urlStorage) String() string {
	return "url://" + u.path
}

func (u *urlStorage) Create() error {
	//dont support
	return nil
}

func (u *urlStorage) Get(key string, off, limit int64) (io.ReadCloser, error) {
	url := strings.Fields(key)[0]
	v, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(v.Body), nil
}

func (u *urlStorage) Put(key string, in io.Reader, _ models.CannedACLType) error {
	//dont support
	return nil
}

func (u *urlStorage) Delete(key string) error {
	//dont support
	return nil
}

func (u *urlStorage) Head(key string) (Object, error) {
	key = strings.Fields(key)[0]
	header, err := httpClient.Head(key)
	if err != nil {
		return nil, err
	}
	return &obj{
		key:  key,
		size: header.ContentLength,
	}, err
}

func (u *urlStorage) List(prefix, marker string, limit int64) ([]Object, error) {
	return nil, notSupported
}

func (u *urlStorage) ListAll(prefix, marker string) (<-chan Object, error) {
	listed := make(chan Object, 10240)
	go func() {
		f, err := os.Open(u.bucket)
		if err != nil {
			logger.Error().Err(err)
		}
		defer f.Close()
		logger.Info().Msgf(u.bucket)
		buf := bufio.NewScanner(f)
		for {
			if !buf.Scan() {
				break
			}
			data := buf.Text()
			dataUrl := strings.Fields(data)[0]
			s := int64(0)
			header, err := httpClient.Head(dataUrl)
			if err == nil {
				s = header.ContentLength
			}
			listed <- &obj{
				key:  data,
				size: s,
			}
		}
		close(listed)
	}()
	return listed, nil
}

func (u *urlStorage) CreateMultipartUpload(key string, minSize int, aclType models.CannedACLType) (*MultipartUpload, error) {
	return nil, notSupported
}

func (u *urlStorage) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	return nil, notSupported
}

func (u *urlStorage) AbortUpload(key string, uploadID string) {
	return
}

func (u *urlStorage) CompleteUpload(key string, uploadID string, parts []*Part) error {
	return notSupported
}

func (u *urlStorage) ListUploads(marker string) ([]*PendingPart, string, error) {
	return nil, "", notSupported
}

func (u *urlStorage) GetObjectAcl(key string) (models.CannedACLType, error) {
	return "", nil
}

func newUrl(filePath, accessKey, secretKey string) (ObjectStorage, error) {
	// For Windows, the path looks like /C:/a/b/c/
	if runtime.GOOS == "windows" && strings.HasPrefix(filePath, "/") {
		filePath = filePath[1:]
	}
	if strings.HasSuffix(filePath, dirSuffix) {
		if err := os.MkdirAll(filePath, 0755); err != nil {
			return nil, fmt.Errorf("creating directory %s failed: %q", filePath, err)
		}
	} else {
		dir := path.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("creating directory %s failed: %q", dir, err)
		}
		_, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("creating directory %s failed: %q", dir, err)
		}
	}
	return &urlStorage{path: filePath, bucket: filePath}, nil
}

func init() {
	Register(models.Url, newUrl)
}
