// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package worker

import (
	"errors"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/sirupsen/logrus"
)

func NewSimple(strategy definition.Strategy) (Worker, error) {
	w := GetWorker(strategy.Bind)
	if w == nil {
		logrus.Warn("Fetch simple worker failed for ", strategy.Bind)
		return nil, errors.New("No specific worker found: " + strategy.Bind)
	}
	logrus.Info("Worker of strategy ", strategy.Id, " created")
	return w, nil
}
