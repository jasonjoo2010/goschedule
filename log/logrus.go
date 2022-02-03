package log

import "github.com/sirupsen/logrus"

type logrusWrapper struct {
	Wrapper
}

// NewLogrusWrapper creates a new wrapper for logrus
func NewLogrusWrapper() Wrapper {
	return &logrusWrapper{}
}

func (w *logrusWrapper) Name() string {
	return "logrus"
}

func (w *logrusWrapper) Debugf(fmt string, args ...interface{}) {
	logrus.Debugf(fmt, args...)
}

func (w *logrusWrapper) Infof(fmt string, args ...interface{}) {
	logrus.Infof(fmt, args...)
}

func (w *logrusWrapper) Warnf(fmt string, args ...interface{}) {
	logrus.Warnf(fmt, args...)
}

func (w *logrusWrapper) Errorf(fmt string, args ...interface{}) {
	logrus.Errorf(fmt, args...)
}

func (w *logrusWrapper) Debug(args ...interface{}) {
	logrus.Debug(args...)
}

func (w *logrusWrapper) Info(args ...interface{}) {
	logrus.Info(args...)
}

func (w *logrusWrapper) Warn(args ...interface{}) {
	logrus.Warn(args...)
}

func (w *logrusWrapper) Error(args ...interface{}) {
	logrus.Error(args...)
}
