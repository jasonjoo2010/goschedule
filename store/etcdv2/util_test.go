// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv2

import (
	"errors"
	"testing"

	etcd "github.com/coreos/etcd/client"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/stretchr/testify/assert"
)

func TestConvertError(t *testing.T) {
	assert.Nil(t, convertError(nil))

	err := errors.New("test")
	assert.Equal(t, err, convertError(err))

	err = etcd.Error{
		Code: etcd.ErrorCodeKeyNotFound,
	}
	assert.Equal(t, store.NotExist, convertError(err))

	err = etcd.Error{
		Code: etcd.ErrorCodeNodeExist,
	}
	assert.Equal(t, store.AlreadyExist, convertError(err))

	err = etcd.Error{
		Code: etcd.ErrorCodeDirNotEmpty,
	}
	assert.Equal(t, err, convertError(err))
}
