package utils

import (
	"mime"
	"path"
	"strings"
	"time"
)

func GuessMimeType(key string) string {
	mimeType := mime.TypeByExtension(path.Ext(key))
	if !strings.ContainsRune(mimeType, '/') {
		mimeType = "application/octet-stream"
	}
	return mimeType
}

func TimeParse(s string, v string) time.Time {
	t := time.Time{}
	if s != "" {
		parse, _ := time.Parse(s, v)
		t = parse
	}
	return t
}
