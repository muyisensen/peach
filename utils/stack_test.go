package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleStack(t *testing.T) {
	ss := NewSimpleStack(5)
	assert.Equal(t, 0, ss.Size())

	elems := make([]interface{}, 0, 10)
	for i := 0; i < 10; i++ {
		ss.Push(i)
		elems = append(elems, i)
	}
	assert.Equal(t, 10, ss.Size())
	assert.Len(t, ss.elems, 10)
	assert.True(t, reflect.DeepEqual(ss.All(), elems))

	size := 9
	for i := 9; i >= 0; i-- {
		assert.True(t, reflect.DeepEqual(ss.Peek(), i))
		assert.True(t, reflect.DeepEqual(ss.Pop(), i))
		assert.Equal(t, size, ss.Size())
		size--
	}
}
