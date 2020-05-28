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
