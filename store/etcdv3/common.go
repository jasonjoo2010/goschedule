// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv3

import (
	"context"
	"encoding/json"
	"errors"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/labstack/gommon/log"
)

func toStr(obj interface{}) (string, error) {
	var val string
	if obj == nil {
		return "", errors.New("Data should not be nil")
	}
	switch v := obj.(type) {
	case string:
		val = v
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		val = string(data)
	}
	return val, nil
}

func (s *Etcdv3Store) exists(path string) bool {
	ctx := context.Background()
	resp, err := s.kvApi.Get(ctx, path)
	if err != nil {
		log.Warnf("Failed to execute Exists(%s): %s", path, err.Error())
		return false
	}
	return resp.Count > 0
}

func (s *Etcdv3Store) remove(path string, recursive bool) error {
	ctx := context.Background()
	resp, err := s.kvApi.Delete(ctx, path)
	if err != nil {
		return err
	}
	if !recursive && resp.Deleted == 0 {
		return store.NotExist
	}
	if recursive {
		_, err = s.kvApi.Delete(ctx, path+"/", etcd.WithPrefix())
	}
	return err
}

func (s *Etcdv3Store) getChildren(base_path string, recursive bool) (map[string]string, error) {
	resp, err := s.kvApi.Get(context.Background(), base_path, etcd.WithPrefix(), etcd.WithLimit(1000))
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}
	return result, nil
}

func (s *Etcdv3Store) create(path string, obj interface{}) error {
	str, err := toStr(obj)
	if err != nil {
		return err
	}
	resp, err := s.kvApi.Txn(context.Background()).
		If(etcd.Compare(etcd.CreateRevision(path), "=", 0)).
		Then(etcd.OpPut(path, str)).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return store.AlreadyExist
	}
	return nil
}

func (s *Etcdv3Store) update(path string, obj interface{}, mustExisted bool) error {
	str, err := toStr(obj)
	if err != nil {
		return err
	}
	if !mustExisted {
		_, err = s.kvApi.Put(context.Background(), path, str)
	} else {
		resp, err := s.kvApi.
			Txn(context.Background()).
			If(etcd.Compare(etcd.CreateRevision(path), "=", 0)).
			Else(etcd.OpPut(path, str)).
			Commit()
		if err != nil {
			return err
		}
		if resp.Succeeded {
			return store.NotExist
		}
	}
	return err
}
