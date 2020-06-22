// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package database

import (
	"encoding/json"
	"errors"
)

func toStr(obj interface{}) (string, error) {
	var val string
	if obj == nil {
		return "", errors.New("Data should not be nil")
	}
	switch v := obj.(type) {
	case string:
		val = v
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		val = string(data)
	}
	return val, nil
}
