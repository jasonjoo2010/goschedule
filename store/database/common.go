// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package database

import (
	"context"
	"errors"

	"github.com/jasonjoo2010/godao"
	"github.com/jasonjoo2010/godao/options"
	"github.com/jasonjoo2010/godao/types"
	"github.com/jasonjoo2010/goschedule/store"
)

func (s *DatabaseStore) updateOrInsert(key string, obj interface{}) error {
	if obj == nil {
		return errors.New("Object should not be nil")
	}
	str, err := toStr(obj)
	if err != nil {
		return err
	}
	affected, err := s.dao.UpdateBy(context.Background(), (&godao.Query{}).
		Equal("Key", key).
		Data(),
		&types.UpdateEntry{
			Field: "Value",
			Value: str,
		},
	)
	if err != nil {
		return err
	}
	if affected < 1 {
		// insert
		_, _, err = s.dao.Insert(context.Background(),
			ScheduleInfo{
				Key:   key,
				Value: str,
			},
			options.WithInsertIgnore(),
		)
	}
	return err
}

func (s *DatabaseStore) create(key string, obj interface{}) error {
	str, err := toStr(obj)
	if err != nil {
		return err
	}
	affected, _, err := s.dao.Insert(context.Background(),
		ScheduleInfo{
			Key:   key,
			Value: str,
		},
		options.WithInsertIgnore(),
	)
	if err != nil {
		return err
	}
	if affected == 0 {
		return store.AlreadyExist
	}
	return nil
}

func (s *DatabaseStore) update(key string, obj interface{}) error {
	if obj == nil {
		return errors.New("Object should not be nil")
	}
	str, err := toStr(obj)
	if err != nil {
		return err
	}
	affected, err := s.dao.UpdateBy(context.Background(), (&godao.Query{}).
		Equal("Key", key).
		Data(),
		&types.UpdateEntry{
			Field: "Value",
			Value: str,
		},
	)
	if err != nil {
		return err
	}
	if affected < 1 {
		return store.NotExist
	}
	return nil
}

func (s *DatabaseStore) remove(key string) error {
	affected, err := s.dao.DeleteRange(context.Background(), (&godao.Query{}).
		Equal("Key", key).
		Data(),
	)
	if affected < 1 {
		return store.NotExist
	}
	return err
}
