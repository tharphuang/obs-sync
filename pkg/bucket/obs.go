package bucket

import (
	"fmt"
	"obs-sync/models"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type obsBucket struct {
	accessKey string
	secretKey string
}

// BucketCreater implements BucketOp.
func (o obsBucket) Create(regino string, name string) error {
	panic("unimplemented")
}

// BucketLister implements BucketOp.
func (o obsBucket) List(region string) ([]BucketInfo, error) {
	var bucketInfos []BucketInfo
	endpoint := fmt.Sprintf("https://obs.%s.myhuaweicloud.com", region)
	cli, err := obs.New(o.accessKey, o.secretKey, endpoint)
	if err != nil {
		return nil, err
	}

	input := obs.ListBucketsInput{QueryLocation: true}
	buckets, err := cli.ListBuckets(&input)
	if err != nil {
		return nil, err
	}

	for _, b := range buckets.Buckets {
		bucketInfos = append(bucketInfos, BucketInfo{
			Type:     models.Obs,
			Name:     b.Name,
			Location: b.Location,
			Domain:   fmt.Sprintf("%s.obs.%s.myhuaweicloud.com", b.Name, b.Location),
		})
	}
	return bucketInfos, nil
}

func (o obsBucket) SetAuth(accessKey, secretKey string) BucketOp {
	o.accessKey = accessKey
	o.secretKey = secretKey
	return o
}

func newObsBucket() BucketOp {
	return obsBucket{}
}

func init() {
	RegisterBucket(models.Obs, newObsBucket())
}
