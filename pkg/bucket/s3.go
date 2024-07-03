package bucket

import (
	"fmt"
	"obs-sync/models"
	"obs-sync/pkg/object"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Bucket struct {
	accessKey string
	secretKey string
}

// BucketCreater implements BucketOp.
func (s s3Bucket) Create(regino string, name string) error {
	panic("unimplemented")
}

// BucketLister implements BucketOp.
func (s s3Bucket) List(region string) ([]BucketInfo, error) {
	input := &s3.ListBucketsInput{}
	//buckets, err := s.s3.ListBuckets(input)
	var bucketInfos []BucketInfo

	awsConfig := &aws.Config{
		HTTPClient:  object.GetHttpClient(),
		Credentials: credentials.NewStaticCredentials(s.accessKey, s.secretKey, ""),
	}
	awsConfig.Region = aws.String(region)
	ses, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("fail to create aws session: %s", err)
	}
	ses.Handlers.Build.PushFront(object.DisableSha256Func)
	service := s3.New(ses)
	listBuckets, err := service.ListBuckets(input)
	if err1, ok := err.(awserr.Error); ok {
		if errCode := err1.Code(); errCode != "InvalidAccessKeyId" && errCode != "InvalidToken" {
			return nil, err
		} else {
			return nil, err
		}
	}
	for _, b := range listBuckets.Buckets {
		l, err := service.GetBucketLocation(&s3.GetBucketLocationInput{
			Bucket: b.Name,
		})
		if err1, ok := err.(awserr.Error); ok {
			// continue to try other regions if the credentials are invalid, otherwise stop trying.
			if errCode := err1.Code(); errCode != "InvalidAccessKeyId" && errCode != "InvalidToken" {
				return nil, err
			}
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Type:     models.S3,
			Name:     *b.Name,
			Location: *l.LocationConstraint,
			Domain:   fmt.Sprintf("%s.%s.amazonaws.com", *b.Name, *l.LocationConstraint),
		})
	}
	if len(bucketInfos) == 0 {
		return nil, fmt.Errorf("can't find any bucket")
	}
	return bucketInfos, nil
}

func (s s3Bucket) SetAuth(accessKey, secretKey string) BucketOp {
	s.accessKey = accessKey
	s.secretKey = secretKey
	return s
}

func newS3Bucket() BucketOp {
	return s3Bucket{}
}

func init() {
	RegisterBucket(models.S3, newS3Bucket())
}
