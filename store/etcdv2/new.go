// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv2

import (
	etcd "github.com/coreos/etcd/client"
	"github.com/sirupsen/logrus"
)

type Option func(cfg *etcd.Config)

func WithCredential(username, password string) Option {
	return func(cfg *etcd.Config) {
		cfg.Username = username
		cfg.Password = password
	}
}

func New(prefix string, addrs []string, opts ...Option) *Etcdv2Store {
	cfg := etcd.Config{
		Endpoints: addrs,
	}
	for _, fn := range opts {
		fn(&cfg)
	}
	c, err := etcd.New(cfg)
	if err != nil {
		logrus.Error("Create etcd store failed: ", err.Error())
		return nil
	}
	store := &Etcdv2Store{
		client:  c,
		keysApi: etcd.NewKeysAPI(c),
		prefix:  prefix,
	}
	store.caculateTimeDifference()
	return store
}
