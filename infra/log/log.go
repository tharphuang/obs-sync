package log

import (
	"io"
	"os"
	"sync"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	defaultLog *Logger
	once       sync.Once
)

func init() {
	once.Do(func() {
		defaultLog = NewLogger("")
	})
}

type Logger struct {
	*zerolog.Logger
	Writer io.Writer
}

func NewLogger(path string) *Logger {
	var logWriter io.Writer
	logWriter = os.Stderr
	if path != "" {
		logWriter = &lumberjack.Logger{
			Filename:   path,
			MaxSize:    512,
			MaxAge:     10,
			MaxBackups: 10,
			LocalTime:  true,
			Compress:   false,
		}
	}
	zLogger := zerolog.New(logWriter).With().Timestamp().Logger()
	return &Logger{&zLogger, logWriter}
}

func (l *Logger) SetLevel(level string) *Logger {
	zLevel := zerolog.InfoLevel
	switch level {
	case "info":
		zLevel = zerolog.InfoLevel
	case "error":
		zLevel = zerolog.ErrorLevel
	case "debug":
		zLevel = zerolog.DebugLevel
	}
	l.Level(zLevel)
	return l
}

func DefaultLogger() *Logger {
	return defaultLog
}
