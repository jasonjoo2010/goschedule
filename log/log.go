package log

var inst = NewLogrusWrapper()

// Wrapper defines the interface used in project to make it able to integrate with different loggers
type Wrapper interface {
	Name() string
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Debugf(fmt string, args ...interface{})
	Infof(fmt string, args ...interface{})
	Warnf(fmt string, args ...interface{})
	Errorf(fmt string, args ...interface{})
}

// Set set the wrapper with a new one
func Set(w Wrapper) {
	if w == nil {
		panic("Cannot initial logging with nil wrapper")
	}
	inst = w
}

// Name shows the name of active wrapper current.
func Name() string {
	return inst.Name()
}

// Debugf is a convenient scaffold for debug logs
func Debugf(fmt string, args ...interface{}) {
	inst.Debugf(fmt, args...)
}

// Infof is a convenient scaffold for information logs
func Infof(fmt string, args ...interface{}) {
	inst.Infof(fmt, args...)
}

// Warnf is a convenient scaffold for warning logs
func Warnf(fmt string, args ...interface{}) {
	inst.Warnf(fmt, args...)
}

// Errorf is a convenient scaffold for error logs
func Errorf(fmt string, args ...interface{}) {
	inst.Errorf(fmt, args...)
}

// Debug is a convenient scaffold for debug logs
func Debug(args ...interface{}) {
	inst.Debug(args...)
}

// Info is a convenient scaffold for information logs
func Info(args ...interface{}) {
	inst.Info(args...)
}

// Warn is a convenient scaffold for warning logs
func Warn(args ...interface{}) {
	inst.Warn(args...)
}

// Error is a convenient scaffold for error logs
func Error(args ...interface{}) {
	inst.Error(args...)
}
