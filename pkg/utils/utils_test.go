package utils

import (
	"testing"
	"time"
)

func TestTimeParse(t *testing.T) {
	t1, _ := time.ParseInLocation("2006-01-02 15:04:05", "2022-08-05 15:04:05", time.Local)
	t.Logf(t1.String())
	parse := TimeParse("2022-08-05 15:04:05", "2006-01-02 15:04:05")
	t.Logf(parse.String())
}
