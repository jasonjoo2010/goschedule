// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"container/list"
	"strings"

	"github.com/jasonjoo2010/goschedule/log"
)

func (s *ZookeeperStore) exists(path string) bool {
	result, _, err := s.conn.Exists(path)
	if err != nil {
		log.Warnf("Failed to execute Exists(%s): ", path, err.Error())
		return false
	}
	return result && err == nil
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
				log.Warnf("Fetch children failed for %s: %s", p, err.Error())
				continue
			}
			for _, child := range children {
				childPath := p + "/" + child
				result = append(result, childPath)
				queue.PushBack(childPath)
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
			log.Warnf("Failed to create path %s: %s", path, err.Error())
			return err
		}
	}
	return nil
}
