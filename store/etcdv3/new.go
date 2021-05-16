// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv3

import (
	"errors"

	etcd "go.etcd.io/etcd/v3/clientv3"
)

type Option func(cfg *etcd.Config)

func WithCredential(username, password string) Option {
	return func(cfg *etcd.Config) {
		cfg.Username = username
		cfg.Password = password
	}
}

func New(prefix string, addrs []string, opts ...Option) (*Etcdv3Store, error) {
	cfg := etcd.Config{
		Endpoints: addrs,
	}
	for _, fn := range opts {
		fn(&cfg)
	}
	c, err := etcd.New(cfg)
	if err != nil {
		return nil, errors.New("Create etcd store failed: " + err.Error())
	}
	store := &Etcdv3Store{
		client:   c,
		kvApi:    etcd.NewKV(c),
		leaseApi: etcd.NewLease(c),
		prefix:   prefix,
	}
	store.caculateTimeDifference()
	return store, nil
}
