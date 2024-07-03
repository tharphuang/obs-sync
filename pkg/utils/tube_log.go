package utils

import (
	"obs-sync/infra/log"
	"sync"
)

var (
	tubeLog *log.Logger
	once    sync.Once
)

func NewTubeLog() {
	once.Do(func() {
		tubeLog = log.NewLogger("./log/tube.log")
	})
}

func TubeLogger() *log.Logger {
	if tubeLog == nil {
		NewTubeLog()
	}
	return tubeLog
}
