package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinkedList(t *testing.T) {
	list := NewLinkedList()
	assert.Equal(t, 0, list.Size())
	assert.Nil(t, list.Head())
	assert.Nil(t, list.Tail())

	array := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		list.Append(i)
		array = append(array, i)
	}
	assert.Equal(t, 10, list.Size())
	assert.True(t, reflect.DeepEqual(0, list.Head()))
	assert.True(t, reflect.DeepEqual(9, list.Tail()))

	curr := list.head.next
	for i := 0; i < 10; i++ {
		assert.True(t, reflect.DeepEqual(curr.value, array[i]))
		curr = curr.next
	}

	list.Truncate(5)
	assert.Equal(t, 5, list.Size())
	curr = list.head.next
	for i := 0; i < 5; i++ {
		assert.True(t, reflect.DeepEqual(curr.value, array[i]))
		curr = curr.next
	}

	assert.True(t, reflect.DeepEqual(0, list.RemoveHead()))
	assert.Equal(t, 4, list.Size())
	assert.True(t, reflect.DeepEqual(4, list.RemoveTail()))
	assert.Equal(t, 3, list.Size())
}
