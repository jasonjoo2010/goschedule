// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package worker

import (
	"errors"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/log"
	"github.com/jasonjoo2010/goschedule/types"
)

func NewSimple(strategy definition.Strategy) (types.Worker, error) {
	w := GetWorker(strategy.Bind)
	if w == nil {
		log.Warnf("Fetch simple worker failed for %s", strategy.Bind)
		return nil, errors.New("No specific worker found: " + strategy.Bind)
	}
	log.Infof("Worker of strategy %s created", strategy.Id)
	return w, nil
}
