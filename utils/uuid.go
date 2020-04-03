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
