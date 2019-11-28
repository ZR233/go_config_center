package log

import "github.com/sirupsen/logrus"

type Logger interface {
	Warn(args ...interface{})
	// Error logs a message at level Error on the standard logger.
	Error(args ...interface{})
}

var logger_ Logger

func init() {
	logger_ = logrus.StandardLogger()
}

func SetLogger(logger Logger) {
	logger_ = logger
}

func Warn(args ...interface{}) {
	logger_.Warn(args...)
}

func Error(args ...interface{}) {
	logger_.Error(args...)
}
