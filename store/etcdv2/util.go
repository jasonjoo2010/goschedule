// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv2

import (
	"github.com/jasonjoo2010/goschedule/store"
	etcd "go.etcd.io/etcd/client"
)

// convertError try to convert error into internal type
func convertError(err error) error {
	if err == nil {
		return nil
	}
	errEtcd, ok := err.(etcd.Error)
	if !ok {
		return err
	}
	if errEtcd.Code == etcd.ErrorCodeKeyNotFound {
		return store.NotExist
	}
	if errEtcd.Code == etcd.ErrorCodeNodeExist {
		return store.AlreadyExist
	}
	return err
}
