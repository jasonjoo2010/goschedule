// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"container/list"
	"strings"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

func (s *ZookeeperStore) exists(path string) bool {
	_, _, err := s.conn.Get(path)
	if err != nil && err.Error() == zk.ErrNoNode.Error() {
		return false
	}
	if err != nil {
		logrus.Warn("Failed to execute Get(", path, "): ", err.Error())
		return false
	}
	return true
}

func (s *ZookeeperStore) getChildren(path string, recursive bool) []string {
	var result []string
	queue := list.New()
	queue.PushBack(strings.TrimRight(path, "/"))
	for queue.Len() > 0 {
		len := queue.Len()
		for i := 0; i < len; i++ {
			item := queue.Front()
			p := item.Value.(string)
			queue.Remove(item)
			children, _, err := s.conn.Children(p)
			if err != nil {
				logrus.Warn("Fetch children failed for ", p, ": ", err.Error())
				continue
			}
			for _, child := range children {
				result = append(result, p+"/"+child)
			}
		}
		if !recursive {
			break
		}
	}
	return result
}

func (s *ZookeeperStore) removePath(path string, recursive bool) error {
	if recursive {
		pathList := s.getChildren(path, true)
		for i := len(pathList) - 1; i >= 0; i-- {
			err := s.conn.Delete(pathList[i], 0)
			if err != nil {
				return err
			}
		}
	}
	return s.conn.Delete(path, 0)
}

func (s *ZookeeperStore) createPath(path string, createParent bool) error {
	if !createParent {
		_, err := s.conn.Create(path, nil, 0, s.acl)
		return err
	}
	b := strings.Builder{}
	for _, str := range splitPath(path) {
		b.WriteString("/")
		b.WriteString(str)
		path := b.String()
		if s.exists(path) {
			continue
		}
		_, err := s.conn.Create(path, nil, 0, s.acl)
		if err != nil {
			logrus.Warn("Failed to create path ", path, ": ", err.Error())
			return err
		}
	}
	return nil
}
