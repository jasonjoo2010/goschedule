package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFetchLeader(t *testing.T) {
	arr := []string{"$000005", "$000000003", "$000004", "$001", "$000002"}
	assert.Equal(t, "$001", FetchLeader(arr))

	arr = []string{"$000001", "$000005", "$000000003", "$000004", "$000002"}
	assert.Equal(t, "$000001", FetchLeader(arr))
}

func TestIsLeader(t *testing.T) {
	arr := []string{"$000005", "$000000003", "$000004", "$001", "$000002"}
	assert.True(t, IsLeader(arr, "$001"))
	assert.False(t, IsLeader(arr, "$002"))
	assert.False(t, IsLeader(arr, "$000000003"))

	arr = []string{"$000001", "$000005", "$000000003", "$000004", "$000002"}
	assert.True(t, IsLeader(arr, "$000001"))
	assert.False(t, IsLeader(arr, "asdf"))
	assert.False(t, IsLeader(arr, "$000002"))
}

func TestOwnSign(t *testing.T) {
	assert.Equal(t, "", OwnSign(""))
	assert.Equal(t, "", OwnSign("test"))
	assert.Equal(t, "Demo", OwnSign("test$Demo"))
	assert.Equal(t, "BASE", OwnSign("test$a$BASE"))
}
