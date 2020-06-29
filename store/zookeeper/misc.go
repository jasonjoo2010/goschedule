// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

func (s *ZookeeperStore) onEvent(event zk.Event) {
	// should not block in this function because it's in synchronous mode
	logrus.Info("recieve: ", event)
}

func (s *ZookeeperStore) determineTimeDelta() {
	// determine the time difference between server & local
	var err error
	keyTime := s.prefix + "/time"
	keyTime, err = s.conn.Create(keyTime, nil, zk.FlagEphemeral|zk.FlagSequence, s.acl)
	if err != nil {
		logrus.Warn("Create testing time key failed: ", err.Error())
	} else {
		now := time.Now().UnixNano() / 1e6
		_, stat, _ := s.conn.Get(keyTime)
		if stat != nil && stat.Ctime > 0 {
			s.timeDelta = time.Duration(stat.Ctime-now) * time.Millisecond
			logrus.Info("Time difference compared to server is ", s.timeDelta)
		}
		s.removePath(keyTime, false)
	}
}

func (s *ZookeeperStore) verifyPrefix() {
	if !s.exists(s.prefix) {
		logrus.Info("Initial goschedule base path")
		err := s.createPath(s.prefix, true)
		if err != nil {
			logrus.Error("Failed to initial base path: ", err.Error())
		}
	}
	s.createPath(s.keySchedulers(), true)
	s.createPath(s.keyTasks(), true)
	s.createPath(s.keyStrategies(), true)
}
