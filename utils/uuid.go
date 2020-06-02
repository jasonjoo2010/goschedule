// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package utils

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// GenerateUUIDPrefix is used to generate the prefix of uuid
func GenerateUUIDPrefix() string {
	return fmt.Sprint(GetHostIPv4(), "$", GetHostName(), "$", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

func GenerateUUID(sequence uint64) string {
	return fmt.Sprintf("%s$%010d", GenerateUUIDPrefix(), sequence)
}
