package cloudstorage

import (
	"fmt"
	"net"
	"net/url"
	"obs-sync/models"
	"obs-sync/pkg/utils"
	"regexp"
	"runtime"
	"strings"

	"obs-sync/pkg/object"
)

// Check if uri is local file path
func isFilePath(uri string) bool {
	// check drive pattern when running on Windows
	if runtime.GOOS == "windows" &&
		len(uri) > 1 && (('a' <= uri[0] && uri[0] <= 'z') ||
		('A' <= uri[0] && uri[0] <= 'Z')) && uri[1] == ':' {
		return true
	}
	return !strings.Contains(uri, ":")
}

func isS3PathType(endpoint string) bool {
	//localhost[:8080] 127.0.0.1[:8080]  s3.ap-southeast-1.amazonaws.com[:8080] s3-ap-southeast-1.amazonaws.com[:8080]
	pattern := `^((localhost)|(s3[.-].*\.amazonaws\.com)|((1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|[1-9])\.((1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.){2}(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)))?(:\d*)?$`
	return regexp.MustCompile(pattern).MatchString(endpoint)
}

func tryCFR() {
	major, minor := utils.GetKernelVersion()
	// copy_file_range() system call first appeared in Linux 4.5, and reworked in 5.3
	// Go requires kernel >= 5.3 to use copy_file_range(), see:
	// https://github.com/golang/go/blob/go1.17.11/src/internal/poll/copy_file_range_linux.go#L58-L66
	if major > 5 || (major == 5 && minor >= 3) {
		object.TryCFR = true
	}
}

func CreateStorage(info models.UriInfo) (object.ObjectStorage, error) {
	bucketDomain := info.BucketDomain
	u, _ := url.Parse(bucketDomain)
	endpoint := u.Path
	// 从联通云,s3的domain里提取bucket
	if info.Type == models.Cuc || info.Type == models.S3 {
		bucket := strings.Split(bucketDomain, ".")[0]
		r := strings.Replace(bucketDomain, bucket+".", "", 1)
		endpoint = r + "/" + bucket
	}

	isS3PathTypeUrl := isS3PathType(endpoint)

	// 本地文件列表，url列表直接复用domain
	if info.Type == models.File || info.Type == models.Url {
		endpoint = bucketDomain
	} else if info.Scheme != "" {
		endpoint = info.Scheme + "://" + endpoint
	} else if supportHTTPS(info.Type, endpoint) {
		endpoint = "https://" + endpoint
	} else {
		endpoint = "http://" + endpoint
	}

	if info.Type == models.S3 && isS3PathTypeUrl {
		// bucket name is part of path
		endpoint += info.BucketDomain
	}

	store, err := object.CreateStorage(info.Type, endpoint, info.AccessKey, info.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("create %s %s: %s", info.Type, endpoint, err)
	}
	return store, nil
}

func supportHTTPS(name models.ResourceType, endpoint string) bool {
	switch name {
	case models.Cuc:
		return !(strings.Contains(endpoint, ".internal-") || strings.Contains(endpoint, "-internal.cucloud.cn"))
	case models.Oss:
		return !(strings.Contains(endpoint, ".vpc100-oss") || strings.Contains(endpoint, "internal.aliyuncs.com"))
	case models.S3:
		ps := strings.SplitN(strings.Split(endpoint, ":")[0], ".", 2)
		if len(ps) > 1 && net.ParseIP(ps[1]) != nil {
			return false
		}
	}
	return true
}
