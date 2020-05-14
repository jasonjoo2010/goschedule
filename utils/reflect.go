package utils

import (
	"reflect"
)

// TypeName returns the "correct" name of type for specified value
func TypeName(obj interface{}) string {
	t := reflect.TypeOf(obj)
	name := t.String()
	star := ""
	if t.Name() == "" {
		if pt := t; t.Kind() == reflect.Ptr {
			star = "*"
			t = pt.Elem()
		}
	}
	// another branch or retry after first part
	if t.Name() != "" {
		if t.PkgPath() == "" {
			name = star + t.Name()
		} else {
			name = star + t.PkgPath() + "." + t.Name()
		}
	}
	return name
}
