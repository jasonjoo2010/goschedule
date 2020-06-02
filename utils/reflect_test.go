// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package utils

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Demo struct {
}

func TestTypeName(t *testing.T) {
	a := Demo{}
	b := &Demo{}
	fmt.Println(TypeName(a))
	fmt.Println(TypeName(b))
}

func TestDereference(t *testing.T) {
	val := time.Now()
	valp := &val
	newVal := Dereference(valp)
	assert.Equal(t, val, newVal)
}
