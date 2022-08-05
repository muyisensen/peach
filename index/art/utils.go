package art

import (
	"reflect"
)

func isNil(no treeNode) bool {
	return no == nil || reflect.ValueOf(no).IsNil()
}

func minimum(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func longestCommonPrefix(a, b []byte) int {
	minLen, i := minimum(len(a), len(b)), 0
	for i < minLen && a[i] == b[i] {
		i++
	}
	return i
}
