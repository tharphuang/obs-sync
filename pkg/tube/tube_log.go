package tube

import (
	"obs-sync/infra/log"
	"sync"
)

var (
	tubeLog *log.Logger
	once    sync.Once
)

func init() {
	once.Do(func() {
		tubeLog = log.NewLogger("./log/tube.log")
	})
}
