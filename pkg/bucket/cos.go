package bucket

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"obs-sync/models"
	"obs-sync/pkg/object"

	"github.com/tencentyun/cos-go-sdk-v5"
)

type cosBucket struct {
	accessKey string
	secretKey string
}

// BucketCreater implements BucketOp.
func (c cosBucket) Create(regino string, name string) error {
	// client := cos.NewClient(nil, &http.Client{
	// 	Transport: &cos.AuthorizationTransport{
	// 		SecretID:  accessKey,
	// 		SecretKey: secretKey,
	// 	},
	// })
	// client.UserAgent = object.UserAgent
	// s, _, err := client.Service.Get(context.Background())
	// if err != nil {
	// 	return nil, err
	// }
	return errors.New("not support")
}

// BucketLister implements BucketOp.
func (c cosBucket) List(region string) ([]BucketInfo, error) {
	client := cos.NewClient(nil, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.accessKey,
			SecretKey: c.secretKey,
		},
	})
	client.UserAgent = object.UserAgent
	s, _, err := client.Service.Get(context.Background())
	if err != nil {
		return nil, err
	}

	var bucketInfos []BucketInfo

	for _, b := range s.Buckets {
		if b.Region == region {
			bucketInfos = append(bucketInfos, BucketInfo{
				Type:     models.Cos,
				Name:     b.Name,
				Location: b.Region,
				Domain:   fmt.Sprintf("%s.cos.%s.myqcloud.com", b.Name, b.Region),
			})
		}
	}
	return bucketInfos, nil
}

func (c cosBucket) SetAuth(accessKey, secretKey string) BucketOp {
	c.accessKey = accessKey
	c.secretKey = secretKey
	return c
}

func NewCosBucket() BucketOp {
	return cosBucket{}
}

func init() {
	RegisterBucket(models.Cos, NewCosBucket())
}
