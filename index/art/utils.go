package art

import "reflect"

func isNil(no treeNode) bool {
	return reflect.ValueOf(no).IsNil()
}

func minimum(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// large common prefix
func largeCommonPerfix(a, b []byte) []byte {
	minLen := minimum(len(a), len(b))

	result := make([]byte, 0, minLen)
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			break
		}
		result = append(result, a[i])
	}
	return result
}
