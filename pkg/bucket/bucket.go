package bucket

import (
	"errors"
	"obs-sync/models"
)

type BucketInfo struct {
	Type     models.ResourceType `json:"type"`
	Name     string              `json:"name"`
	Location string              `json:"location"`
	Domain   string              `json:"domain"`
}

type BucketOp interface {
	SetAuth(accessKey, secretKey string) BucketOp
	List(region string) ([]BucketInfo, error)
	Create(regino, name string) error
}

var buckets = make(map[models.ResourceType]BucketOp)

func RegisterBucket(name models.ResourceType, register BucketOp) {
	buckets[name] = register
}

type defaultBucket struct{}

// Create implements BucketOp.
func (d defaultBucket) Create(regino string, name string) error {
	return errors.New("unimplemented")
}

// List implements BucketOp.
func (d defaultBucket) List(region string) ([]BucketInfo, error) {
	return nil, errors.New("unimplemented")
}

// SetAuth implements BucketOp.
func (d defaultBucket) SetAuth(accessKey string, secretKey string) BucketOp {
	return nil
}

func BucketStorage(name models.ResourceType, accessKey, secretKey string) BucketOp {
	b, ok := buckets[name]
	if ok {
		return b.SetAuth(accessKey, secretKey)
	}
	return defaultBucket{}
}
