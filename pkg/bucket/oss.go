package bucket

import (
	"fmt"
	"obs-sync/models"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type ossBucket struct {
	accessKey string
	secretKey string
}

// BucketCreater implements BucketOp.
func (o ossBucket) Create(regino string, name string) error {
	panic("unimplemented")
}

// BucketLister implements BucketOp.
func (o ossBucket) List(region string) ([]BucketInfo, error) {
	var bucketInfos []BucketInfo
	defaultEndpoint := fmt.Sprintf("https://oss-%s.aliyuncs.com", region)

	client, err := oss.New(defaultEndpoint, o.accessKey, o.secretKey)
	if err != nil {
		return nil, err
	}

	marker := ""
	for {
		lsRes, err := client.ListBuckets(oss.Marker(marker))
		if err != nil {
			return nil, err
		}

		for _, bucket := range lsRes.Buckets {
			bucketInfos = append(bucketInfos, BucketInfo{
				Type:     models.Oss,
				Name:     bucket.Name,
				Location: bucket.Location,
				Domain:   fmt.Sprintf("%s.%s.aliyuncs.com", bucket.Name, bucket.Location),
			})
		}
		if lsRes.IsTruncated {
			marker = lsRes.NextMarker
		} else {
			break
		}
	}
	return bucketInfos, nil
}

func (o ossBucket) SetAuth(accessKey, secretKey string) BucketOp {
	o.accessKey = accessKey
	o.secretKey = secretKey
	return o
}

func newOssBucket() BucketOp {
	return ossBucket{}
}

func init() {
	RegisterBucket(models.Oss, newOssBucket())
}
