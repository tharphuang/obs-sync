package models

type TaskConfig struct {
	TaskName                  string        `toml:"taskName"`
	SrcType                   ResourceType  `toml:"srcType"`
	SrcAccessKey              string        `toml:"srcAccessKey"`
	SrcSecretKey              string        `toml:"srcSecretKey"`
	SrcDomain                 string        `toml:"srcDomain"`
	SrcScheme                 string        `toml:"srcScheme"`
	SrcBucket                 string        `toml:"srcBucket"`
	SrcPrefix                 string        `toml:"srcPrefix"`
	SrcFileName               string        `toml:"srcFileName"`
	SrcStart                  string        `toml:"srcStart"`
	SrcEnd                    string        `toml:"srcEnd"`
	DestType                  ResourceType  `toml:"destType"`
	DestAccessKey             string        `toml:"destAccessKey"`
	DestSecretKey             string        `toml:"destSecretKey"`
	DestDomain                string        `toml:"destDomain"`
	DestScheme                string        `toml:"destScheme"`
	DestBucket                string        `toml:"destBucket"`
	DestPrefix                string        `toml:"destPrefix"`
	CosAppID                  string        `toml:"cosAppID"`
	Filters                   []string      `toml:"filters"`
	MaxThroughput             int           `toml:"maxThroughput"`
	CannedAcl                 CannedACLType `toml:"cannedAcl"`
	ModifyTimeRange           string        `toml:"modifyTimeRange"`
	MultipartUploadThreshold  int           `toml:"multipartUploadThreshold"`
	MultipartUploadPartSize   int           `toml:"multipartUploadPartSize"`
	MaxNetThroughputTimeRange string        `toml:"maxNetThroughputTimeRange"`
	MaxNetThroughputRange     string        `toml:"maxNetThroughputRange"`
	IncrementalMode           bool          `toml:"incrementalMode"`
	IncrementalModeInterval   int           `toml:"incrementalModeInterval"`
	IncrementalModeCount      int           `toml:"incrementalModeCount"`
	IsSkipExistFile           int           `toml:"isSkipExistFile"`
	SetObjectMetaMD5          bool          `toml:"setObjectMetaMD5"`
	SrcMD5Header              string        `toml:"srcMD5Header"`
}
