// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv2

import (
	"context"
	"time"

	"github.com/labstack/gommon/log"
	etcd "go.etcd.io/etcd/client"
)

func (s *Etcdv2Store) caculateTimeDifference() {
	resp, err := s.keysApi.CreateInOrder(context.Background(), s.prefix, "", &etcd.CreateInOrderOptions{
		TTL: 10 * time.Second,
	})
	if err != nil {
		log.Warnf("Create node failed: %s", err.Error())
		return
	}
	s.timeDelta = resp.Node.Expiration.Add(-10 * time.Second).Sub(time.Now())
	s.keysApi.Delete(context.Background(), resp.Node.Key, nil)
}
