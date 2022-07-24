package utils

import (
	"os"
	"time"
)

func PTime(t time.Time) *time.Time {
	return &t
}

func Exist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
