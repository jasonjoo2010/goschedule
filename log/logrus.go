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

func (w *logrusWrapper) Debug(fmt string) {
	logrus.Debugf(fmt)
}

func (w *logrusWrapper) Info(fmt string) {
	logrus.Infof(fmt)
}

func (w *logrusWrapper) Warn(fmt string) {
	logrus.Warnf(fmt)
}

func (w *logrusWrapper) Error(fmt string) {
	logrus.Errorf(fmt)
}
