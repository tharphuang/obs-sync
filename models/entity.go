package models

// ResourceType 云产商资源类型
type ResourceType string

const (
	File ResourceType = "file"
	Oss  ResourceType = "oss"
	S3   ResourceType = "s3"
	Cuc  ResourceType = "cuc"
	Obs  ResourceType = "obs"
	Cos  ResourceType = "cos"
	Url  ResourceType = "url"
)

type Orientation int

const (
	To   Orientation = 1 // -->
	From Orientation = 2 // <--
	With Orientation = 3 // <->
)

// StatsType Handle:1, Skipped:10, Copied:11, Failed:100
type StatsType int64

const (
	Scanned StatsType = 1
	Skipped StatsType = 2
	Copied  StatsType = 3
	Failed  StatsType = 4
	Size    StatsType = 5
	Finish  StatsType = 6
)

type CannedACLType string

const (
	Default           CannedACLType = "default"
	Private           CannedACLType = "private"
	PublicRead        CannedACLType = "public-read"
	AuthenticatedRead CannedACLType = "authenticated-read"
	PublicReadWrite   CannedACLType = "public-read-write"
)

type Obj struct {
	Key   string
	Size  int64
	Mtime int64
	IsDir bool
}
type Task struct {
	BuckeNmae string
	SrcInfo   UriInfo
	DestInfo  UriInfo
	Objs      []Obj
}

type TaskInfo struct {
	Name       string     `json:"name"`
	Status     string     `json:"status"`
	Info       TaskConfig `json:"info"`
	StatsData  Stats      `json:"statsData"`
	CurrentKey string     `json:"currentKey"`
}

type UriInfo struct {
	Type         ResourceType `json:"type"`
	Scheme       string       `json:"scheme"`
	BucketDomain string       `json:"bucketDomain"`
	AccessKey    string       `json:"accessKey"`
	SecretKey    string       `json:"secretKey"`
}

type Stats struct {
	Scanned, Skipped, Copied, Failed, Size int64
	FinishFlag                             bool
}

type Uri struct {
	Type      ResourceType `json:"type"`
	AccessKey string       `json:"accessKey"`
	SecretKey string       `json:"secretKey"`
	Region    string       `json:"region"`
}

type BucketOri struct {
	SrcBucket   string      `json:"srcBucket"`
	Orientation Orientation `json:"to"`
	DestBucket  string      `json:"destBucket"`
	Name        string      `json:"name"`
}

func (b BucketOri) Ori() string {
	switch b.Orientation {
	case To:
		return "==>"
	case From:
		return "<=="
	case With:
		return "<=>"
	}
	return ""
}

type SyncInfo struct {
	SrcUri      Uri         `json:"srcUri"`
	DestUri     Uri         `json:"destUri"`
	BucketRanks []BucketOri `json:"bucketRanks"`
}
