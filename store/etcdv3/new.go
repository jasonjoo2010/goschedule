// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv3

import (
	"errors"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/jasonjoo2010/enhanced-utils/concurrent/distlock"
	lockstore "github.com/jasonjoo2010/enhanced-utils/concurrent/distlock/etcdv3"
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
	var lockopts []lockstore.Option
	lockopts = append(lockopts, lockstore.WithTTL(10))
	lockopts = append(lockopts, lockstore.WithPrefix(prefix))
	if cfg.Username != "" {
		lockopts = append(lockopts, lockstore.WithCredential(cfg.Username, cfg.Password))
	}
	lockStore, err := lockstore.New(addrs, lockopts...)
	if err != nil {
		return nil, errors.New("Failed to create dstributed lock on etcdv3: " + err.Error())
	}
	store := &Etcdv3Store{
		client:   c,
		kvApi:    etcd.NewKV(c),
		leaseApi: etcd.NewLease(c),
		prefix:   prefix,
		lock:     distlock.NewMutex("lock", 10*time.Second, lockStore),
	}
	store.caculateTimeDifference()
	return store, nil
}
