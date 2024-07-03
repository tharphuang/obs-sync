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

type cucBucket struct {
	accessKey string
	secretKey string
}

// BucketCreater implements BucketOp.
func (c cucBucket) Create(region string, name string) error {
	input := &s3.CreateBucketInput{
		Bucket: &name,
	}

	awsConfig := &aws.Config{
		HTTPClient:  object.GetHttpClient(),
		Credentials: credentials.NewStaticCredentials(c.accessKey, c.secretKey, ""),
		Region:      aws.String(region),
		Endpoint:    aws.String(fmt.Sprintf("https://obs-%s.cucloud.cn", region)),
	}
	ses, err := session.NewSession(awsConfig)
	if err != nil {
		return fmt.Errorf("fail to create aws session: %s", err)
	}
	service := s3.New(ses)
	_, err = service.CreateBucket(input)
	return err
}

// BucketLister implements BucketOp.
func (c cucBucket) List(region string) ([]BucketInfo, error) {
	input := &s3.ListBucketsInput{}
	var bucketInfos []BucketInfo

	awsConfig := &aws.Config{
		HTTPClient:  object.GetHttpClient(),
		Credentials: credentials.NewStaticCredentials(c.accessKey, c.secretKey, ""),
	}
	awsConfig.Region = aws.String(region)
	awsConfig.Endpoint = aws.String(fmt.Sprintf("https://obs-%s.cucloud.cn", region))
	awsConfig.DisableEndpointHostPrefix = aws.Bool(true)
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
		bucketInfos = append(bucketInfos, BucketInfo{
			Type:     models.Cuc,
			Name:     *b.Name,
			Location: region,
			Domain:   fmt.Sprintf("%s.obs-%s.cucloud.cn", *b.Name, region),
		})

	}

	return bucketInfos, nil
}

func (c cucBucket) SetAuth(accessKey, secretKey string) BucketOp {
	c.accessKey = accessKey
	c.secretKey = secretKey
	return c
}

func newCuCBucket() BucketOp {
	return cucBucket{}
}

func init() {
	RegisterBucket(models.Cuc, newCuCBucket())
}
