// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package database

import (
	"database/sql"

	"github.com/jasonjoo2010/godao"
	"github.com/jasonjoo2010/godao/options"
)

type ScheduleConfig struct {
	InfoTable string
}

type Option func(cfg *ScheduleConfig)

type ScheduleInfo struct {
	Id      int64 `dao:"primary;auto_increment"`
	Key     string
	Value   string
	Version int64
}

func WithInfoTable(table string) Option {
	return func(cfg *ScheduleConfig) {
		cfg.InfoTable = table
	}
}

func New(namespace string, db *sql.DB, opts ...Option) *DatabaseStore {
	cfg := ScheduleConfig{}
	for _, fn := range opts {
		fn(&cfg)
	}
	if cfg.InfoTable == "" {
		cfg.InfoTable = "schedule_info"
	}

	store := &DatabaseStore{
		db:        db,
		dao:       godao.NewDao(ScheduleInfo{}, db, options.WithTable(cfg.InfoTable)),
		namespace: namespace,
	}
	return store
}
