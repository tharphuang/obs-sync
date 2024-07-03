//go:build !nocuc
// +build !nocuc

package object

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"obs-sync/models"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	mapset "github.com/deckarep/golang-set"
)

const cucChecksumKeyPrefix = "x-amz-meta-cuoss-"

type Cuc struct {
	bucket       string
	s3           *s3.S3
	ses          *session.Session
	checkSumKey  string
	sumAlgorithm algorithm
}

func (c *Cuc) SetCheckSumKey(meta string) error {
	c.checkSumKey = meta
	return nil
}

func (c *Cuc) IsSetMd5(flag bool) error {
	if flag {
		c.sumAlgorithm = checksumMd5
	}
	return nil
}

func (c *Cuc) String() string {
	return fmt.Sprintf("cuc://%s/", c.bucket)
}

func (c *Cuc) Create() error {
	if _, err := c.List("", "", 1); err == nil {
		return nil
	}
	_, err := c.s3.CreateBucket(&s3.CreateBucketInput{Bucket: &c.bucket})
	if err != nil && isExists(err) {
		err = nil
	}
	return err
}

func (c *Cuc) Head(key string) (Object, error) {
	param := s3.HeadObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	}
	r, err := c.s3.HeadObject(&param)
	if err != nil {
		return nil, err
	}
	return &obj{
		key,
		*r.ContentLength,
		*r.LastModified,
		strings.HasSuffix(key, "/"),
	}, nil
}

func (c *Cuc) Get(key string, off, limit int64) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{Bucket: &c.bucket, Key: &key}
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("bytes=%d-", off)
		}
		params.Range = &r
	}
	resp, err := c.s3.GetObject(params)
	if err != nil {
		return nil, err
	}
	if off == 0 && limit == -1 {
		cs := resp.Metadata[c.checkSumKey]
		if cs != nil {
			resp.Body = verifyChecksum(resp.Body, *cs)
		}
	}
	return resp.Body, nil
}

func (c *Cuc) Put(key string, in io.Reader, acl models.CannedACLType) error {
	var body io.ReadSeeker
	if b, ok := in.(io.ReadSeeker); ok {
		body = b
	} else {
		data, err := ioutil.ReadAll(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	checkSumMetaKey := "cuoss-" + c.sumAlgorithm.String()
	checksum := generateChecksum(body, c.sumAlgorithm)
	params := &s3.PutObjectInput{
		Bucket:   &c.bucket,
		Key:      &key,
		Body:     body,
		Metadata: map[string]*string{checkSumMetaKey: &checksum},
	}
	if acl == models.Default {
		acl = c.getBucketAcl()
	}
	params.ACL = aws.String(string(acl))
	_, err := c.s3.PutObject(params)
	return err
}

func (c *Cuc) Copy(dst, src string) error {
	src = c.bucket + "/" + src
	params := &s3.CopyObjectInput{
		Bucket:     &c.bucket,
		Key:        &dst,
		CopySource: &src,
	}
	_, err := c.s3.CopyObject(params)
	return err
}

func (c *Cuc) Delete(key string) error {
	param := s3.DeleteObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	}
	_, err := c.s3.DeleteObject(&param)
	return err
}

func (c *Cuc) List(prefix, marker string, limit int64) ([]Object, error) {
	param := s3.ListObjectsInput{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(prefix),
		Marker:  aws.String(marker),
		MaxKeys: aws.Int64(limit),
	}
	resp, err := c.s3.ListObjects(&param)
	if err != nil {
		return nil, err
	}
	n := len(resp.Contents)
	objs := make([]Object, n)
	for i := 0; i < n; i++ {
		o := resp.Contents[i]
		objs[i] = &obj{
			*o.Key,
			*o.Size,
			*o.LastModified,
			strings.HasSuffix(*o.Key, "/"),
		}
	}
	return objs, nil
}

func (c *Cuc) ListAll(prefix, marker string) (<-chan Object, error) {
	return nil, notSupported
}

func (c *Cuc) CreateMultipartUpload(key string, minSize int, acl models.CannedACLType) (*MultipartUpload, error) {
	params := &s3.CreateMultipartUploadInput{
		Bucket: &c.bucket,
		Key:    &key,
	}
	if acl == models.Default {
		acl = c.getBucketAcl()
	}
	params.ACL = aws.String(string(acl))
	resp, err := c.s3.CreateMultipartUpload(params)
	if err != nil {
		return nil, err
	}
	return &MultipartUpload{UploadID: *resp.UploadId, MinPartSize: minSize, MaxCount: 10000}, nil
}

func (c *Cuc) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	n := int64(num)
	params := &s3.UploadPartInput{
		Bucket:     &c.bucket,
		Key:        &key,
		UploadId:   &uploadID,
		Body:       bytes.NewReader(body),
		PartNumber: &n,
	}
	resp, err := c.s3.UploadPart(params)
	if err != nil {
		return nil, err
	}
	return &Part{Num: num, ETag: *resp.ETag}, nil
}

func (c *Cuc) AbortUpload(key string, uploadID string) {
	params := &s3.AbortMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      &key,
		UploadId: &uploadID,
	}
	_, _ = c.s3.AbortMultipartUpload(params)
}

func (c *Cuc) CompleteUpload(key string, uploadID string, parts []*Part) error {
	var s3Parts []*s3.CompletedPart
	for i := range parts {
		n := new(int64)
		*n = int64(parts[i].Num)
		s3Parts = append(s3Parts, &s3.CompletedPart{ETag: &parts[i].ETag, PartNumber: n})
	}
	params := &s3.CompleteMultipartUploadInput{
		Bucket:          &c.bucket,
		Key:             &key,
		UploadId:        &uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: s3Parts},
	}
	_, err := c.s3.CompleteMultipartUpload(params)
	return err
}

func (c *Cuc) ListUploads(marker string) ([]*PendingPart, string, error) {
	input := &s3.ListMultipartUploadsInput{
		Bucket:    aws.String(c.bucket),
		KeyMarker: aws.String(marker),
	}

	result, err := c.s3.ListMultipartUploads(input)
	if err != nil {
		return nil, "", err
	}
	parts := make([]*PendingPart, len(result.Uploads))
	for i, u := range result.Uploads {
		parts[i] = &PendingPart{*u.Key, *u.UploadId, *u.Initiated}
	}
	var nextMarker string
	if result.NextKeyMarker != nil {
		nextMarker = *result.NextKeyMarker
	}
	return parts, nextMarker, nil
}

func (c *Cuc) getBucketAcl() models.CannedACLType {
	inputBucket := &s3.GetBucketAclInput{
		Bucket: &c.bucket,
	}
	result, err := c.s3.GetBucketAcl(inputBucket)
	if err != nil {
		return ""
	}
	allUsersPermissions := mapset.NewSet()
	authenticatedRead := false
	for _, value := range result.Grants {
		if value.Grantee.URI != nil && strings.Contains(*value.Grantee.URI, "AllUsers") {
			allUsersPermissions.Add(aws.StringValue(value.Permission))
		}
		if strings.Contains(aws.StringValue(value.Grantee.URI), "AuthenticatedUsers") && aws.StringValue(value.Permission) == "READ" {
			authenticatedRead = true
		}
	}
	read := allUsersPermissions.Contains("READ")
	write := allUsersPermissions.Contains("WRITE")
	if read && write { // all user read write
		return models.PublicReadWrite
	} else if read { // all read
		return models.PublicRead
	} else if authenticatedRead {
		return models.AuthenticatedRead
	} else { //owner FULL_CONTROL
		return models.Private
	}
}

func (c *Cuc) GetObjectAcl(key string) (models.CannedACLType, error) {
	// object acl
	input := &s3.GetObjectAclInput{
		Bucket: &c.bucket,
		Key:    &key,
	}
	result, err := c.s3.GetObjectAcl(input)
	if err != nil || len(result.Grants) == 0 {
		return "", err
	}
	// bucket acl
	inputBucket := &s3.GetBucketAclInput{
		Bucket: &c.bucket,
	}
	resultBucket, err := c.s3.GetBucketAcl(inputBucket)
	if err != nil || len(resultBucket.Grants) == 0 {
		return "", err
	}
	if resultBucket.String() == result.String() {
		return models.Default, nil
	}

	allUsersPermissions := mapset.NewSet()
	authenticatedRead := false
	for _, value := range result.Grants {
		if value.Grantee.URI != nil && strings.Contains(*value.Grantee.URI, "AllUsers") {
			allUsersPermissions.Add(aws.StringValue(value.Permission))
		}
		if strings.Contains(aws.StringValue(value.Grantee.URI), "AuthenticatedUsers") && aws.StringValue(value.Permission) == "READ" {
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

func parseCucRegion(endpoint string) string {
	if strings.HasPrefix(endpoint, "obs-") || strings.HasPrefix(endpoint, "obs.") {
		endpoint = endpoint[4:]
	}
	region := strings.Split(endpoint, ".")[0]
	return region
}

func newCuc(endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	endpoint = strings.Trim(endpoint, "/")
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %s: %v", endpoint, err.Error())
	}

	var (
		bucketName string
		region     string
		ep         string
	)

	if uri.Path != "" {
		// [ENDPOINT]/[BUCKET]
		pathParts := strings.Split(uri.Path, "/")
		bucketName = pathParts[1]
		if strings.Contains(uri.Host, ".cucloud.cn") {
			// standard s3
			// s3-[REGION].[REST_OF_ENDPOINT]/[BUCKET]
			// s3.[REGION].amazonaws.com[.cn]/[BUCKET]
			endpoint = uri.Host
			region = parseCucRegion(endpoint)
			ep = uri.Scheme + "://" + uri.Host
		} else {
			// compatible s3
			ep = uri.Host
			region = "lf-1"
		}
	} else {
		return nil, fmt.Errorf("can't guess your region for bucket %s: %s", bucketName, err)
	}

	ssl := strings.ToLower(uri.Scheme) == "https"
	awsConfig := &aws.Config{
		Region:     aws.String(region),
		DisableSSL: aws.Bool(!ssl),
		HTTPClient: httpClient,
	}

	if accessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}
	if ep != "" {
		awsConfig.Endpoint = aws.String(ep)
		awsConfig.S3ForcePathStyle = aws.Bool(true)
	}

	ses, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("fail to create aws session: %s", err)
	}

	ses.Handlers.Build.PushFront(DisableSha256Func)
	return &Cuc{bucketName, s3.New(ses), ses, cucChecksumKeyPrefix + checksumCrc32.String(), checksumCrc32}, nil
}

func init() {
	Register(models.Cuc, newCuc)
}
