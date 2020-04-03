package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetHostIPv4(t *testing.T) {
	assert.NotEmpty(t, GetHostIPv4())
}

func TestGetHostName(t *testing.T) {
	assert.NotEmpty(t, GetHostName())
}
