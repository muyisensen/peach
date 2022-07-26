package art

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/muyisensen/peach/index"
	"github.com/stretchr/testify/assert"
)

type pair struct {
	key   []byte
	value *index.MemValue
}

type pairs []*pair

// Len is the number of elements in the collection.
func (kv pairs) Len() int {
	return len(kv)
}

func (kv pairs) Less(i int, j int) bool {
	switch bytes.Compare(kv[i].key, kv[j].key) {
	case -1:
		return true
	default:
		return false
	}
}

// Swap swaps the elements with indexes i and j.
func (kv pairs) Swap(i int, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

func TestIterator(t *testing.T) {
	tree := NewAdaptiveRadixTree(&index.AdaptiveRadixTreeOptions{
		NodeLeafPoolSize: 8,
		Node4PoolSize:    8,
		Node16PoolSize:   8,
		Node48PoolSize:   8,
		Node256PoolSize:  8,
	})

	kv := make([]*pair, 0, 100)
	for i := 0; i < 100; i++ {
		key := []byte(uuid.New().String())
		value := &index.MemValue{
			FileID: i,
			Offset: int64(i * 10),
			Size:   i * 100,
		}
		tree.Put(key, value)
		kv = append(kv, &pair{key: []byte(key), value: value})
	}

	sort.Sort(pairs(kv))
	it := tree.Iterate()

	i := 0
	for it.HasNext() {
		k, v := it.Next()
		k1, v1 := kv[i].key, kv[i].value
		assert.True(t, reflect.DeepEqual(k, k1))
		assert.True(t, reflect.DeepEqual(v, v1))
		i++
	}
}
