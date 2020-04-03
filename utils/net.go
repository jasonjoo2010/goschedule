package utils

import (
	"net"
	"os"
	"strings"
)

// GetHostIPv4 returns localhost's ip in ipv4. If failed return "127.0.0.1"
// It's suggested that the hostname is resolved to the correct address.
func GetHostIPv4() string {
	hostname := GetHostName()
	addrs, err := net.LookupHost(hostname)
	if err != nil || len(addrs) == 0 {
		return "127.0.0.1"
	}
	for _, ip := range addrs {
		if strings.Contains(ip, ":") {
			continue
		}
		return ip
	}
	return "127.0.0.1"
}

// GetHostName returns localhost's hostname. Return "localhost" for fallback
func GetHostName() string {
	h, err := os.Hostname()
	if err != nil {
		return "localhost"
	}
	return h
}
