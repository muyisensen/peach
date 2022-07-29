package art

import (
	"reflect"
	"testing"

	"github.com/muyisensen/peach/index"
	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	tree := NewAdaptiveRadixTree(&index.AdaptiveRadixTreeOptions{
		NodeLeafPoolSize: 8,
		Node4PoolSize:    8,
		Node16PoolSize:   8,
		Node48PoolSize:   8,
		Node256PoolSize:  8,
	})

	key := []byte("hello")
	assert.Nil(t, tree.Get(key))
	value := &index.MemValue{
		FileID: 1,
		Offset: 10,
		Size:   100,
	}
	assert.Nil(t, tree.Put(key, value))
	assert.Equal(t, int64(1), tree.Size())
	assert.True(t, reflect.DeepEqual(value, tree.Get(key)))
	assert.Nil(t, tree.Get([]byte("abc")))

	otherKey := []byte("hel")
	assert.Nil(t, tree.Put(otherKey, value))
	assert.Equal(t, int64(2), tree.Size())
	assert.True(t, reflect.DeepEqual(value, tree.Get(otherKey)))

	key2 := []byte("abc")
	value2 := &index.MemValue{
		FileID: 2,
		Offset: 20,
		Size:   200,
	}
	assert.Nil(t, tree.Put(key2, value2))
	assert.Equal(t, int64(3), tree.Size())
	assert.True(t, reflect.DeepEqual(value2, tree.Get(key2)))

	assert.True(t, reflect.DeepEqual(value, tree.Put(key, value2)))
	assert.Equal(t, int64(3), tree.Size())
	assert.True(t, reflect.DeepEqual(value2, tree.Get(key)))

	assert.Nil(t, tree.Delete([]byte("nil")))
	assert.Equal(t, int64(3), tree.Size())
	assert.True(t, reflect.DeepEqual(value2, tree.Delete(key)))
	assert.Equal(t, int64(2), tree.Size())

	minKey, minValue := tree.Minimum()
	assert.True(t, reflect.DeepEqual(minKey, key2))
	assert.True(t, reflect.DeepEqual(value2, minValue))
	maxKey, maxValue := tree.Maximum()
	assert.True(t, reflect.DeepEqual(maxKey, otherKey))
	assert.True(t, reflect.DeepEqual(maxValue, value))
}
