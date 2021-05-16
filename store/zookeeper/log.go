package zookeeper

import "github.com/jasonjoo2010/goschedule/log"

type logger struct {
}

func (l *logger) Printf(format string, args ...interface{}) {
	log.Infof(format, args)
}
