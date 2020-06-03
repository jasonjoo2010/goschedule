// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import "strings"

func splitPath(path string) []string {
	arr := strings.Split(path, "/")
	// shrink empty element
	cur := 0
	for i := range arr {
		if arr[i] == "" {
			continue
		}
		arr[cur] = arr[i]
		cur++
	}
	return arr[:cur]
}
