//go:build !noobs
// +build !noobs

package object

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"obs-sync/models"
	"os"
	"strings"

	mapset "github.com/deckarep/golang-set"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"golang.org/x/net/http/httpproxy"

	"obs-sync/pkg/utils"
)

const obsDefaultRegion = "cn-north-1"

type obsClient struct {
	bucket string
	region string
	c      *obs.ObsClient
}

func (o *obsClient) SetCheckSumKey(meta string) error {
	return notSupported
}

func (o *obsClient) IsSetMd5(flag bool) error {
	return notSupported
}

func (o *obsClient) String() string {
	return fmt.Sprintf("obs://%s/", o.bucket)
}

func (o *obsClient) Create() error {
	params := &obs.CreateBucketInput{}
	params.Bucket = o.bucket
	params.Location = o.region
	_, err := o.c.CreateBucket(params)
	if err != nil && isExists(err) {
		err = nil
	}
	return err
}

func (o *obsClient) Head(key string) (Object, error) {
	params := &obs.GetObjectMetadataInput{
		Bucket: o.bucket,
		Key:    key,
	}
	r, err := o.c.GetObjectMetadata(params)
	if err != nil {
		return nil, err
	}
	return &obj{
		key,
		r.ContentLength,
		r.LastModified,
		strings.HasSuffix(key, "/"),
	}, nil
}

func (o *obsClient) Get(key string, off, limit int64) (io.ReadCloser, error) {
	params := &obs.GetObjectInput{}
	params.Bucket = o.bucket
	params.Key = key
	params.RangeStart = off
	if limit > 0 {
		params.RangeEnd = off + limit - 1
	}
	resp, err := o.c.GetObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (o *obsClient) Put(key string, in io.Reader, acl models.CannedACLType) error {
	var body io.ReadSeeker
	var vlen int64
	var sum []byte
	if b, ok := in.(io.ReadSeeker); ok {
		var err error
		h := md5.New()
		buf := bufPool.Get().(*[]byte)
		defer bufPool.Put(buf)
		vlen, err = io.CopyBuffer(h, in, *buf)
		if err != nil {
			return err
		}
		_, err = b.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		sum = h.Sum(nil)
		body = b
	} else {
		data, err := ioutil.ReadAll(in)
		if err != nil {
			return err
		}
		vlen = int64(len(data))
		s := md5.Sum(data)
		sum = s[:]
		body = bytes.NewReader(data)
	}
	mimeType := utils.GuessMimeType(key)
	params := &obs.PutObjectInput{}
	if acl != "" && acl != models.Default {
		params.ACL = obs.AclType(acl)
	}
	params.Bucket = o.bucket
	params.Key = key
	params.Body = body
	params.ContentLength = vlen
	params.ContentMD5 = base64.StdEncoding.EncodeToString(sum[:])
	params.ContentType = mimeType
	_, err := o.c.PutObject(params)
	return err
}

func (o *obsClient) Copy(dst, src string) error {
	params := &obs.CopyObjectInput{}
	params.Bucket = o.bucket
	params.Key = dst
	params.CopySourceBucket = o.bucket
	params.CopySourceKey = src
	_, err := o.c.CopyObject(params)
	return err
}

func (o *obsClient) Delete(key string) error {
	params := obs.DeleteObjectInput{}
	params.Bucket = o.bucket
	params.Key = key
	_, err := o.c.DeleteObject(&params)
	return err
}

func (o *obsClient) List(prefix, marker string, limit int64) ([]Object, error) {
	input := &obs.ListObjectsInput{
		Bucket: o.bucket,
		Marker: marker,
	}
	input.Prefix = prefix
	input.MaxKeys = int(limit)
	resp, err := o.c.ListObjects(input)
	if err != nil {
		return nil, err
	}
	n := len(resp.Contents)
	objs := make([]Object, n)
	for i := 0; i < n; i++ {
		o := resp.Contents[i]
		objs[i] = &obj{o.Key, o.Size, o.LastModified, strings.HasSuffix(o.Key, "/")}
	}
	return objs, nil
}

func (o *obsClient) ListAll(prefix, marker string) (<-chan Object, error) {
	return nil, notSupported
}

func (o *obsClient) CreateMultipartUpload(key string, minSize int, acl models.CannedACLType) (*MultipartUpload, error) {
	params := &obs.InitiateMultipartUploadInput{}
	if acl != "" && acl != models.Default {
		params.ACL = obs.AclType(acl)
	}
	params.Bucket = o.bucket
	params.Key = key
	resp, err := o.c.InitiateMultipartUpload(params)
	if err != nil {
		return nil, err
	}
	return &MultipartUpload{UploadID: resp.UploadId, MinPartSize: minSize, MaxCount: 10000}, nil
}

func (o *obsClient) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	params := &obs.UploadPartInput{}
	params.Bucket = o.bucket
	params.Key = key
	params.UploadId = uploadID
	params.Body = bytes.NewReader(body)
	params.PartNumber = num
	params.PartSize = int64(len(body))
	sum := md5.Sum(body)
	params.ContentMD5 = base64.StdEncoding.EncodeToString(sum[:])
	resp, err := o.c.UploadPart(params)
	if err != nil {
		return nil, err
	}
	return &Part{Num: num, ETag: resp.ETag}, nil
}

func (o *obsClient) AbortUpload(key string, uploadID string) {
	params := &obs.AbortMultipartUploadInput{}
	params.Bucket = o.bucket
	params.Key = key
	params.UploadId = uploadID
	_, _ = o.c.AbortMultipartUpload(params)
}

func (o *obsClient) CompleteUpload(key string, uploadID string, parts []*Part) error {
	params := &obs.CompleteMultipartUploadInput{}
	params.Bucket = o.bucket
	params.Key = key
	params.UploadId = uploadID
	for i := range parts {
		params.Parts = append(params.Parts, obs.Part{ETag: parts[i].ETag, PartNumber: parts[i].Num})
	}
	_, err := o.c.CompleteMultipartUpload(params)
	return err
}

func (o *obsClient) ListUploads(marker string) ([]*PendingPart, string, error) {
	input := &obs.ListMultipartUploadsInput{
		Bucket:    o.bucket,
		KeyMarker: marker,
	}

	result, err := o.c.ListMultipartUploads(input)
	if err != nil {
		return nil, "", err
	}
	parts := make([]*PendingPart, len(result.Uploads))
	for i, u := range result.Uploads {
		parts[i] = &PendingPart{u.Key, u.UploadId, u.Initiated}
	}
	var nextMarker string
	if result.NextKeyMarker != "" {
		nextMarker = result.NextKeyMarker
	}
	return parts, nextMarker, nil
}

func (o *obsClient) GetObjectAcl(key string) (models.CannedACLType, error) {
	// object acl
	input := &obs.GetObjectAclInput{
		Bucket: o.bucket,
		Key:    key,
	}
	result, err := o.c.GetObjectAcl(input)
	if err != nil || len(result.Grants) == 0 {
		return "", err
	}
	// bucket acl
	resultBucket, err := o.c.GetBucketAcl(o.bucket)
	if err != nil || len(resultBucket.Grants) == 0 {
		return "", err
	}
	if fmt.Sprintf("%v", resultBucket.Grants) == fmt.Sprintf("%v", result.Grants) {
		return models.Default, nil
	}

	allUsersPermissions := mapset.NewSet()
	authenticatedRead := false
	for _, value := range result.Grants {
		if value.Grantee.URI == obs.GroupAllUsers {
			allUsersPermissions.Add(value.Permission)
		}
		if value.Grantee.URI == obs.GroupAuthenticatedUsers && value.Permission == obs.PermissionRead {
			authenticatedRead = true
		}
	}
	read := allUsersPermissions.Contains("READ")
	write := allUsersPermissions.Contains("WRITE")
	if read && write { // all user read write
		return models.PublicReadWrite, nil
	} else if read { // all read
		return models.PublicRead, nil
	} else if authenticatedRead {
		return models.AuthenticatedRead, nil
	} else { //owner FULL_CONTROL
		return models.Private, nil
	}
}

func autoOBSEndpoint(bucketName, accessKey, secretKey string) (string, error) {
	region := obsDefaultRegion
	if r := os.Getenv("HWCLOUD_DEFAULT_REGION"); r != "" {
		region = r
	}
	endpoint := fmt.Sprintf("https://obs.%s.myhuaweicloud.com", region)

	obsCli, err := obs.New(accessKey, secretKey, endpoint)
	if err != nil {
		return "", err
	}
	defer obsCli.Close()

	result, err := obsCli.ListBuckets(&obs.ListBucketsInput{QueryLocation: true})
	if err != nil {
		return "", err
	}
	for _, bucket := range result.Buckets {
		if bucket.Name == bucketName {
			logger.Debug().Msgf("Get location of bucket %q: %s", bucketName, bucket.Location)
			return fmt.Sprintf("obs.%s.myhuaweicloud.com", bucket.Location), nil
		}
	}
	return "", fmt.Errorf("bucket %q does not exist", bucketName)
}

func newOBS(endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = fmt.Sprintf("https://%s", endpoint)
	}
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %s: %q", endpoint, err)
	}
	hostParts := strings.SplitN(uri.Host, ".", 2)
	bucketName := hostParts[0]
	if len(hostParts) > 1 {
		endpoint = fmt.Sprintf("%s://%s", uri.Scheme, hostParts[1])
	}

	if accessKey == "" {
		accessKey = os.Getenv("HWCLOUD_ACCESS_KEY")
		secretKey = os.Getenv("HWCLOUD_SECRET_KEY")
	}

	var region string
	if len(hostParts) == 1 {
		if endpoint, err = autoOBSEndpoint(bucketName, accessKey, secretKey); err != nil {
			return nil, fmt.Errorf("cannot get location of bucket %s: %q", bucketName, err)
		}
		if !strings.HasPrefix(endpoint, "http") {
			endpoint = fmt.Sprintf("%s://%s", uri.Scheme, endpoint)
		}
	} else {
		region = strings.Split(hostParts[1], ".")[1]
	}

	// Use proxy setting from environment variables: HTTP_PROXY, HTTPS_PROXY, NO_PROXY
	if uri, err = url.ParseRequestURI(endpoint); err != nil {
		return nil, fmt.Errorf("invalid endpoint %s: %q", endpoint, err)
	}
	proxyURL, err := httpproxy.FromEnvironment().ProxyFunc()(uri)
	if err != nil {
		return nil, fmt.Errorf("get proxy url for endpoint: %s error: %q", endpoint, err)
	}
	var urlString string
	if proxyURL != nil {
		urlString = proxyURL.String()
	}

	// Empty proxy url string has no effect
	// there is a bug in the retry of PUT (did not call Seek(0,0) before retry), so disable the retry here
	c, err := obs.New(accessKey, secretKey, endpoint, obs.WithProxyUrl(urlString), obs.WithMaxRetryCount(0))
	if err != nil {
		return nil, fmt.Errorf("fail to initialize OBS: %q", err)
	}
	return &obsClient{bucketName, region, c}, nil
}

func init() {
	Register(models.Obs, newOBS)
}
