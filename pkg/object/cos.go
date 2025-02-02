//go:build !nocos
// +build !nocos

package object

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"obs-sync/models"
	"strconv"
	"strings"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
)

const cosChecksumKeyPrefix = "x-cos-meta-"

type COS struct {
	c            *cos.Client
	endpoint     string
	checkSumKey  string
	sumAlgorithm algorithm
}

func (c *COS) SetCheckSumKey(meta string) error {
	c.checkSumKey = meta
	return nil
}

func (c *COS) IsSetMd5(flag bool) error {
	if flag {
		c.sumAlgorithm = checksumMd5
	}
	return nil
}

func (c *COS) String() string {
	return fmt.Sprintf("cos://%s/", strings.Split(c.endpoint, ".")[0])
}

func (c *COS) Create() error {
	_, err := c.c.Bucket.Put(ctx, nil)
	if err != nil && isExists(err) {
		err = nil
	}
	return err
}

func (c *COS) Head(key string) (Object, error) {
	resp, err := c.c.Object.Head(ctx, key, nil)
	if err != nil {
		return nil, err
	}

	header := resp.Header
	var size int64
	if val, ok := header["Content-Length"]; ok {
		if length, err := strconv.ParseInt(val[0], 10, 64); err == nil {
			size = length
		}
	}
	var mtime time.Time
	if val, ok := header["Last-Modified"]; ok {
		mtime, _ = time.Parse(time.RFC1123, val[0])
	}
	return &obj{key, size, mtime, strings.HasSuffix(key, "/")}, nil
}

func (c *COS) Get(key string, off, limit int64) (io.ReadCloser, error) {
	params := &cos.ObjectGetOptions{}
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("bytes=%d-", off)
		}
		params.Range = r
	}
	resp, err := c.c.Object.Get(ctx, key, params)
	if err != nil {
		return nil, err
	}
	if off == 0 && limit == -1 {
		resp.Body = verifyChecksum(resp.Body, resp.Header.Get(c.checkSumKey))
	}
	return resp.Body, nil
}

func (c *COS) Put(key string, in io.Reader, acl models.CannedACLType) error {
	var options *cos.ObjectPutOptions
	checkSumMetaKey := cosChecksumKeyPrefix + c.sumAlgorithm.String()
	if ins, ok := in.(io.ReadSeeker); ok {
		header := http.Header(map[string][]string{
			checkSumMetaKey: {generateChecksum(ins, c.sumAlgorithm)},
		})
		// cos 默认权限不支持 prw https://cloud.tencent.com/document/product/436/30752#.E6.93.8D.E4.BD.9C-permission
		if acl == "" || acl == models.PublicReadWrite {
			acl = models.Default
		}
		options = &cos.ObjectPutOptions{
			ACLHeaderOptions:       &cos.ACLHeaderOptions{XCosACL: string(acl)},
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XCosMetaXXX: &header},
		}
	}
	_, err := c.c.Object.Put(ctx, key, in, options)
	return err
}

func (c *COS) Copy(dst, src string) error {
	source := fmt.Sprintf("%s/%s", c.endpoint, src)
	_, _, err := c.c.Object.Copy(ctx, dst, source, nil)
	return err
}

func (c *COS) Delete(key string) error {
	_, err := c.c.Object.Delete(ctx, key)
	return err
}

func (c *COS) List(prefix, marker string, limit int64) ([]Object, error) {
	param := cos.BucketGetOptions{
		Prefix:  prefix,
		Marker:  marker,
		MaxKeys: int(limit),
	}
	resp, _, err := c.c.Bucket.Get(ctx, &param)
	for err == nil && len(resp.Contents) == 0 && resp.IsTruncated {
		param.Marker = resp.NextMarker
		resp, _, err = c.c.Bucket.Get(ctx, &param)
	}
	if err != nil {
		return nil, err
	}
	n := len(resp.Contents)
	objs := make([]Object, n)
	for i := 0; i < n; i++ {
		o := resp.Contents[i]
		t, _ := time.Parse(time.RFC3339, o.LastModified)
		objs[i] = &obj{o.Key, int64(o.Size), t, strings.HasSuffix(o.Key, "/")}
	}
	return objs, nil
}

func (c *COS) ListAll(prefix, marker string) (<-chan Object, error) {
	return nil, notSupported
}

func (c *COS) CreateMultipartUpload(key string, minSize int, acl models.CannedACLType) (*MultipartUpload, error) {
	var options *cos.InitiateMultipartUploadOptions
	if acl != models.Default && acl != "" {
		options = &cos.InitiateMultipartUploadOptions{
			ACLHeaderOptions: &cos.ACLHeaderOptions{XCosACL: string(acl)},
		}
	}
	resp, _, err := c.c.Object.InitiateMultipartUpload(ctx, key, options)
	if err != nil {
		return nil, err
	}
	return &MultipartUpload{UploadID: resp.UploadID, MinPartSize: minSize, MaxCount: 10000}, nil
}

func (c *COS) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	resp, err := c.c.Object.UploadPart(ctx, key, uploadID, num, bytes.NewReader(body), nil)
	if err != nil {
		return nil, err
	}
	return &Part{Num: num, ETag: resp.Header.Get("Etag")}, nil
}

func (c *COS) AbortUpload(key string, uploadID string) {
	_, _ = c.c.Object.AbortMultipartUpload(ctx, key, uploadID)
}

func (c *COS) CompleteUpload(key string, uploadID string, parts []*Part) error {
	var cosParts []cos.Object
	for i := range parts {
		cosParts = append(cosParts, cos.Object{Key: key, ETag: parts[i].ETag, PartNumber: parts[i].Num})
	}
	_, _, err := c.c.Object.CompleteMultipartUpload(ctx, key, uploadID, &cos.CompleteMultipartUploadOptions{Parts: cosParts})
	return err
}

func (c *COS) ListUploads(marker string) ([]*PendingPart, string, error) {
	input := &cos.ListMultipartUploadsOptions{
		KeyMarker: marker,
	}
	result, _, err := c.c.Bucket.ListMultipartUploads(ctx, input)
	if err != nil {
		return nil, "", err
	}
	parts := make([]*PendingPart, len(result.Uploads))
	for i, u := range result.Uploads {
		t, _ := time.Parse(time.RFC3339, u.Initiated)
		parts[i] = &PendingPart{u.Key, u.UploadID, t}
	}
	return parts, result.NextKeyMarker, nil
}

func (c *COS) GetObjectAcl(key string) (models.CannedACLType, error) {
	_, response, err := c.c.Object.GetACL(ctx, key)
	if err != nil {
		return "", err
	}
	acl := response.Header.Get("x-cos-acl")
	switch acl {
	case "private":
		return models.Private, nil
	case "public-read-write":
		return models.PublicReadWrite, nil
	case "public-read":
		return models.PublicRead, nil
	case "authenticated-read":
		return models.AuthenticatedRead, nil
	default:
		return models.Default, nil
	}
}

//func (c *COS) SetObjectAcl(key string, acl CannedACLType) error {
//	c.c.Object.PutACL(ctx, key, acl)
//}

func autoCOSEndpoint(bucketName, accessKey, secretKey string) (string, error) {
	client := cos.NewClient(nil, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  accessKey,
			SecretKey: secretKey,
		},
	})
	client.UserAgent = UserAgent
	s, _, err := client.Service.Get(ctx)
	if err != nil {
		return "", err
	}

	for _, b := range s.Buckets {
		// fmt.Printf("%#v\n", b)
		if b.Name == bucketName {
			return fmt.Sprintf("https://%s.cos.%s.myqcloud.com", b.Name, b.Region), nil
		}
	}

	return "", fmt.Errorf("bucket %q doesnot exist", bucketName)
}

func newCOS(endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = fmt.Sprintf("https://%s", endpoint)
	}
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %s: %s", endpoint, err)
	}
	hostParts := strings.SplitN(uri.Host, ".", 2)

	if len(hostParts) == 1 {
		if endpoint, err = autoCOSEndpoint(hostParts[0], accessKey, secretKey); err != nil {
			return nil, fmt.Errorf("unable to get endpoint of bucket %s: %s", hostParts[0], err)
		}
		if uri, err = url.ParseRequestURI(endpoint); err != nil {
			return nil, fmt.Errorf("invalid endpoint %s: %s", endpoint, err)
		}
		logger.Debug().Msgf("Use endpoint %q", endpoint)
	}

	b := &cos.BaseURL{BucketURL: uri}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  accessKey,
			SecretKey: secretKey,
			Transport: httpClient.Transport,
		},
	})
	client.UserAgent = UserAgent
	return &COS{client, uri.Host, cosChecksumKeyPrefix + checksumCrc32.String(), checksumCrc32}, nil
}

func init() {
	Register(models.Cos, newCOS)
}
