package cloudstorage

import (
	"obs-sync/models"
	"obs-sync/pkg/bucket"
	"testing"
)

func TestCuc(t *testing.T) {

	srcUri := models.UriInfo{
		Type:         models.Cuc,
		BucketDomain: "myoms.obs-hehehe.cucloud.cn",
		AccessKey:    "hehehe",
		SecretKey:    "hehehe",
	}

	storage, err := CreateStorage(srcUri)
	if err != nil {
		t.Logf("创建联通云失败：%s", err.Error())
	}

	list, err := storage.List("", "", 100)
	//acl, err := storage.GetObjectAcl("图片2.png")
	//t.Logf("%v", acl)
	if err != nil {
		t.Logf("list obj error:%s", err.Error())
	}
	for _, o := range list {
		t.Logf("测试key：%v", o.Key())
	}

	bucketer := bucket.BucketStorage("ll", srcUri.AccessKey, srcUri.SecretKey)
	buckets, err := bucketer.List("helf")
	if err != nil {
		t.Logf("buclet err:%v", err)
	}

	for _, b := range buckets {
		t.Logf("bucket:%v", b)
	}
}
