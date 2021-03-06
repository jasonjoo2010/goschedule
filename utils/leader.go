// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package utils

import (
	"strconv"
	"strings"
)

func getSequence(uuid string) uint64 {
	pos := strings.LastIndexByte(uuid, '$')
	if pos < 0 {
		return 0
	}
	n, err := strconv.Atoi(uuid[pos+1:])
	if err != nil {
		return 0
	}
	return uint64(n)
}

// FetchLeader returns the uuid of leader in the array
func FetchLeader(uuids []string) string {
	if len(uuids) < 1 {
		return ""
	}
	if len(uuids) == 1 {
		return uuids[0]
	}
	uuid := uuids[0]
	seq := getSequence(uuids[0])
	for i := 1; i < len(uuids); i++ {
		n := getSequence(uuids[i])
		if n < seq {
			seq = n
			uuid = uuids[i]
		}
	}
	return uuid
}

// IsLeader tells whether specified uuid is the leader in the uuid slice
func IsLeader(uuids []string, uuid string) bool {
	return uuid == FetchLeader(uuids)
}

// OwnSign gets ownsign from strategyId, "" for no ownsign
func OwnSign(strategyId string) string {
	pos := strings.LastIndex(strategyId, "$")
	if pos > 0 {
		return strategyId[pos+1:]
	}
	return ""
}
