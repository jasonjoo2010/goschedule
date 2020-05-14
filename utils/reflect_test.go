package utils

import (
	"fmt"
	"testing"
)

type Demo struct {
}

func TestTypeName(t *testing.T) {
	a := Demo{}
	b := &Demo{}
	fmt.Println(TypeName(a))
	fmt.Println(TypeName(b))
}
