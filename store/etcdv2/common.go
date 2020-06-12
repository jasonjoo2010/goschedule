// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv2

import (
	"context"
	"encoding/json"
	"errors"

	etcd "github.com/coreos/etcd/client"
	"github.com/sirupsen/logrus"
)

func (s *Etcdv2Store) exists(path string) bool {
	ctx := context.Background()
	_, err := s.keysApi.Get(ctx, path, &etcd.GetOptions{})
	if err != nil {
		if errEtcd, ok := err.(etcd.Error); ok {
			if errEtcd.Code == etcd.ErrorCodeKeyNotFound {
				return false
			}
		}
		logrus.Warn("Failed to execute Exists(", path, "): ", err.Error())
		return false
	}
	return true
}

func (s *Etcdv2Store) remove(path string, recursive bool) error {
	_, err := s.keysApi.Delete(context.Background(), path, &etcd.DeleteOptions{
		Recursive: recursive,
	})
	return convertError(err)
}

func dump(result map[string]string, nodes etcd.Nodes) {
	for _, n := range nodes {
		if n.Dir {
			dump(result, n.Nodes)
		}
		if n.Value != "" {
			result[n.Key] = n.Value
		}
	}
}

func (s *Etcdv2Store) getChildren(base_path string, recursive bool) (map[string]string, error) {
	resp, err := s.keysApi.Get(context.Background(), base_path, &etcd.GetOptions{
		Recursive: recursive,
	})
	if err != nil {
		if errEtcd, ok := err.(etcd.Error); ok && errEtcd.Code == etcd.ErrorCodeKeyNotFound {
			return make(map[string]string, 0), nil
		}
		return nil, convertError(err)
	}
	result := make(map[string]string, len(resp.Node.Nodes))
	dump(result, resp.Node.Nodes)
	return result, nil
}

func (s *Etcdv2Store) create(path string, obj interface{}) error {
	var val string
	if obj == nil {
		return errors.New("Data should not be nil")
	}
	switch v := obj.(type) {
	case string:
		val = v
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		val = string(data)
	}
	_, err := s.keysApi.Create(context.Background(), path, val)
	return convertError(err)
}

func (s *Etcdv2Store) update(path string, obj interface{}, mustExisted bool) error {
	var val string
	if obj == nil {
		return errors.New("Data should not be nil")
	}
	switch v := obj.(type) {
	case string:
		val = v
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		val = string(data)
	}
	t := etcd.PrevExist
	if !mustExisted {
		t = etcd.PrevIgnore
	}
	_, err := s.keysApi.Set(context.Background(), path, val, &etcd.SetOptions{
		PrevExist: t,
	})
	return convertError(err)
}

func (s *Etcdv2Store) verify() {

}
