// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"time"

	"github.com/jasonjoo2010/goschedule/log"
	"github.com/samuel/go-zookeeper/zk"
)

func (s *ZookeeperStore) onEvent(event zk.Event) {
	// should not block in this function because it's in synchronous mode
	log.Infof("receive: %s", event.State.String())
}

func (s *ZookeeperStore) determineTimeDelta() {
	// determine the time difference between server & local
	var err error
	keyTime := s.prefix + "/time"
	keyTime, err = s.conn.Create(keyTime, nil, zk.FlagEphemeral|zk.FlagSequence, s.acl)
	if err != nil {
		log.Warnf("Create testing time key failed: %s", err.Error())
	} else {
		now := time.Now().UnixNano() / 1e6
		_, stat, _ := s.conn.Get(keyTime)
		if stat != nil && stat.Ctime > 0 {
			s.timeDelta = time.Duration(stat.Ctime-now) * time.Millisecond
			log.Infof("Time difference compared to server is %v", s.timeDelta)
		}
		s.removePath(keyTime, false)
	}
}

func (s *ZookeeperStore) verifyPrefix() {
	if !s.exists(s.prefix) {
		log.Info("Initial goschedule base path")
		err := s.createPath(s.prefix, true)
		if err != nil {
			log.Errorf("Failed to initial base path: %s", err.Error())
		}
	}
	s.createPath(s.keySchedulers(), true)
	s.createPath(s.keyTasks(), true)
	s.createPath(s.keyStrategies(), true)
}
