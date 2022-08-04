package utils

import (
	"math/rand"
	"os"
	"time"
)

const (
	letterIdxBits = 8
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
)

var src = rand.NewSource(time.Now().UnixNano())

func RandBytes(n int) []byte {
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < 256 {
			b[i] = byte(idx)
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return b
}

func Exist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
