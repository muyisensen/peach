package art

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/muyisensen/peach/index"
	"github.com/stretchr/testify/assert"
)

func TestNodeLeaf(t *testing.T) {
	leaf := &nodeLeaf{}
	assert.Nil(t, leaf.Key())
	assert.Nil(t, leaf.Value())

	key := []byte("hello")
	value := &index.MemValue{
		Offset: 0,
		Size:   100,
		FileID: 1,
	}
	leaf.SetKey(key)
	leaf.SetValue(value)

	assert.True(t, reflect.DeepEqual(leaf.Key(), key))
	assert.True(t, reflect.DeepEqual(leaf.Value(), value))
}

func TestNode4(t *testing.T) {
	node := &node4{}
	assert.Zero(t, node.NumOfChild())

	assert.Len(t, node.Key(), 0)
	key := []byte("abc")
	node.SetKey(key)
	assert.True(t, reflect.DeepEqual(node.Key(), key))

	childs := []treeNode{}
	for i := 1; i <= 5; i++ {
		key := []byte{byte(i), byte(i + 10)}
		value := &index.MemValue{FileID: i}
		childs = append(childs, &nodeLeaf{key: key, value: value})
	}
	rand.Shuffle(5, func(i, j int) {
		childs[i], childs[j] = childs[j], childs[i]
	})

	var notSuccess treeNode
	for i, item := range childs {
		child := item
		if i == 4 {
			assert.False(t, node.InsertChild(child))
			notSuccess = child
		} else {
			assert.True(t, node.InsertChild(child))
		}
	}
	assert.True(t, node.InsertChild(&nodeLeaf{key: nil, value: &index.MemValue{FileID: -1}}))

	assert.Equal(t, 4, node.NumOfChild())
	assert.False(t, isNil(node.zeroLeaf))
	var lastChar byte
	for _, char := range node.keys {
		assert.True(t, lastChar < char)
		lastChar = char
	}

	for i := 1; i <= 5; i++ {
		key := []byte{byte(i), byte(i + 10)}
		if bytes.Equal(key, notSuccess.Key()) {
			assert.True(t, node.FindChild(key) == nil)
		} else {
			assert.False(t, node.FindChild(key) == nil)
		}
	}

	childs = node.ListAllChild()
	assert.Len(t, childs, 5)
	var lastKey []byte
	for i, item := range childs {
		child := item
		if i == 0 {
			assert.Equal(t, 0, bytes.Compare(child.Key(), lastKey))
		} else {
			assert.Equal(t, 1, bytes.Compare(child.Key(), lastKey))
		}
		lastKey = child.Key()
	}

	min := (*node.FirstChild())
	assert.Equal(t, -1, min.Value().FileID)

	child := node.RemoveChild(nil)
	assert.Equal(t, -1, child.Value().FileID)

	min = (*node.FirstChild())
	assert.True(t, bytes.HasPrefix(min.Key(), []byte{node.keys[0]}))

	max := (*node.LastChild())
	assert.True(t, bytes.HasPrefix(max.Key(), []byte{node.keys[node.numOfChild-1]}))

	child = node.RemoveChild(max.Key())
	assert.True(t, reflect.DeepEqual(max, child))
	assert.True(t, node.FindChild(max.Key()) == nil)

	max = (*node.LastChild())
	assert.True(t, bytes.HasPrefix(max.Key(), []byte{node.keys[node.numOfChild-1]}))
}

func TestNode16(t *testing.T) {
	node := &node16{}
	assert.Zero(t, node.NumOfChild())

	assert.Len(t, node.Key(), 0)
	key := []byte("abc")
	node.SetKey(key)
	assert.True(t, reflect.DeepEqual(node.Key(), key))

	childs := []treeNode{}
	for i := 1; i <= 17; i++ {
		key := []byte{byte(i), byte(i + 10)}
		value := &index.MemValue{FileID: i}
		childs = append(childs, &nodeLeaf{key: key, value: value})
	}
	rand.Shuffle(17, func(i, j int) {
		childs[i], childs[j] = childs[j], childs[i]
	})

	var notSuccess treeNode
	for i, item := range childs {
		child := item
		if i == 16 {
			assert.False(t, node.InsertChild(child))
			notSuccess = child
		} else {
			assert.True(t, node.InsertChild(child))
		}
	}
	assert.True(t, node.InsertChild(&nodeLeaf{key: nil, value: &index.MemValue{FileID: -1}}))

	assert.Equal(t, 16, node.NumOfChild())
	assert.False(t, isNil(node.zeroLeaf))
	var lastChar byte
	for _, char := range node.keys {
		assert.True(t, lastChar < char)
		lastChar = char
	}

	for i := 1; i <= 17; i++ {
		key := []byte{byte(i), byte(i + 10)}
		if bytes.Equal(key, notSuccess.Key()) {
			assert.True(t, node.FindChild(key) == nil)
		} else {
			assert.False(t, node.FindChild(key) == nil)
		}
	}

	childs = node.ListAllChild()
	assert.Len(t, childs, 17)
	var lastKey []byte
	for i, item := range childs {
		child := item
		if i == 0 {
			assert.Equal(t, 0, bytes.Compare(child.Key(), lastKey))
		} else {
			assert.Equal(t, 1, bytes.Compare(child.Key(), lastKey))
		}
		lastKey = child.Key()
	}

	min := (*node.FirstChild())
	assert.Equal(t, -1, min.Value().FileID)

	child := node.RemoveChild(nil)
	assert.Equal(t, -1, child.Value().FileID)

	min = (*node.FirstChild())
	assert.True(t, bytes.HasPrefix(min.Key(), []byte{node.keys[0]}))

	max := (*node.LastChild())
	assert.True(t, bytes.HasPrefix(max.Key(), []byte{node.keys[node.numOfChild-1]}))

	child = node.RemoveChild(max.Key())
	assert.True(t, reflect.DeepEqual(max, child))
	assert.True(t, node.FindChild(max.Key()) == nil)

	max = (*node.LastChild())
	assert.True(t, bytes.HasPrefix(max.Key(), []byte{node.keys[node.numOfChild-1]}))
}

func TestNode48(t *testing.T) {
	node := &node48{}
	assert.Zero(t, node.NumOfChild())

	assert.Len(t, node.Key(), 0)
	key := []byte("abc")
	node.SetKey(key)
	assert.True(t, reflect.DeepEqual(node.Key(), key))

	childs := []treeNode{}
	for i := 1; i <= 49; i++ {
		key := []byte{byte(i), byte(i + 10)}
		value := &index.MemValue{FileID: i}
		childs = append(childs, &nodeLeaf{key: key, value: value})
	}
	rand.Shuffle(49, func(i, j int) {
		childs[i], childs[j] = childs[j], childs[i]
	})

	var notSuccess treeNode
	var presents [4]uint64
	for k, item := range childs {
		child := item
		if k == 48 {
			assert.False(t, node.InsertChild(child))
			notSuccess = child
		} else {
			assert.True(t, node.InsertChild(child))
			i, j := child.Key()[0]>>6, child.Key()[0]%64
			presents[i] |= (1 << j)
		}
	}
	assert.True(t, node.InsertChild(&nodeLeaf{key: nil, value: &index.MemValue{FileID: -1}}))
	assert.Equal(t, 48, node.NumOfChild())
	assert.False(t, isNil(node.zeroLeaf))
	assert.True(t, reflect.DeepEqual(node.presents, presents))

	for i := 1; i <= 49; i++ {
		key := []byte{byte(i), byte(i + 10)}
		if bytes.Equal(key, notSuccess.Key()) {
			assert.True(t, node.FindChild(key) == nil)
		} else {
			assert.False(t, node.FindChild(key) == nil)
		}
	}

	childs = node.ListAllChild()
	assert.Len(t, childs, 49)
	var lastKey []byte
	for i, item := range childs {
		child := item
		if i == 0 {
			assert.Equal(t, 0, bytes.Compare(child.Key(), lastKey))
		} else {
			assert.Equal(t, 1, bytes.Compare(child.Key(), lastKey))
		}
		lastKey = child.Key()
	}

	min := (*node.FirstChild())
	assert.Equal(t, -1, min.Value().FileID)

	child := node.RemoveChild(nil)
	assert.Equal(t, -1, child.Value().FileID)

	firstChar := uint8(0)
	for char := 0; char < 256; char++ {
		i, j := char>>6, char%64
		if node.presents[i]&(1<<j) != 0 {
			firstChar = uint8(char)
			break
		}
	}

	min = (*node.FirstChild())
	assert.True(t, reflect.DeepEqual(min, node.children[node.keys[firstChar]]))

	lastChar := uint8(0)
	for char := 255; char >= 0; char-- {
		i, j := char>>6, char%64
		if node.presents[i]&(1<<j) != 0 {
			lastChar = uint8(char)
			break
		}
	}

	max := (*node.LastChild())
	assert.True(t, reflect.DeepEqual(max, node.children[node.keys[lastChar]]))

	child = node.RemoveChild(max.Key())
	assert.True(t, reflect.DeepEqual(max, child))
	assert.True(t, node.FindChild(max.Key()) == nil)

	lastChar = uint8(0)
	for char := 255; char >= 0; char-- {
		i, j := char>>6, char%64
		if node.presents[i]&(1<<j) != 0 {
			lastChar = uint8(char)
			break
		}
	}

	max = (*node.LastChild())
	assert.True(t, reflect.DeepEqual(max, node.children[node.keys[lastChar]]))
}

func TestNode256(t *testing.T) {
	node := &node256{}
	assert.Zero(t, node.NumOfChild())

	assert.Len(t, node.Key(), 0)
	key := []byte("abc")
	node.SetKey(key)
	assert.True(t, reflect.DeepEqual(node.Key(), key))

	childs := []treeNode{}
	for i := 0; i < 256; i++ {
		key := []byte{byte(i), byte(i)}
		value := &index.MemValue{FileID: i}
		childs = append(childs, &nodeLeaf{key: key, value: value})
	}
	rand.Shuffle(256, func(i, j int) {
		childs[i], childs[j] = childs[j], childs[i]
	})

	var presents [4]uint64
	for _, item := range childs {
		child := item
		assert.True(t, node.InsertChild(child))
		i, j := child.Key()[0]>>6, child.Key()[0]%64
		presents[i] |= (1 << j)
	}
	assert.True(t, node.InsertChild(&nodeLeaf{key: nil, value: &index.MemValue{FileID: -1}}))
	assert.Equal(t, 256, node.NumOfChild())
	assert.False(t, isNil(node.zeroLeaf))
	assert.True(t, reflect.DeepEqual(node.presents, presents))

	for i := 0; i < 256; i++ {
		key := []byte{byte(i), byte(i)}
		assert.False(t, node.FindChild(key) == nil)
	}

	childs = node.ListAllChild()
	assert.Len(t, childs, 257)
	var lastKey []byte
	for i, item := range childs {
		child := item
		if i == 0 {
			assert.Equal(t, 0, bytes.Compare(child.Key(), lastKey))
		} else {
			assert.Equal(t, 1, bytes.Compare(child.Key(), lastKey))
		}
		lastKey = child.Key()
	}

	min := (*node.FirstChild())
	assert.Equal(t, -1, min.Value().FileID)

	child := node.RemoveChild(nil)
	assert.Equal(t, -1, child.Value().FileID)

	firstChar := uint8(0)
	for char := 0; char < 256; char++ {
		i, j := char>>6, char%64
		if node.presents[i]&(1<<j) != 0 {
			firstChar = uint8(char)
			break
		}
	}

	min = (*node.FirstChild())
	assert.True(t, reflect.DeepEqual(min, node.children[firstChar]))

	lastChar := uint8(0)
	for char := 255; char >= 0; char-- {
		i, j := char>>6, char%64
		if node.presents[i]&(1<<j) != 0 {
			lastChar = uint8(char)
			break
		}
	}

	max := (*node.LastChild())
	assert.True(t, reflect.DeepEqual(max, node.children[lastChar]))

	child = node.RemoveChild(max.Key())
	assert.True(t, reflect.DeepEqual(max, child))
	assert.True(t, node.FindChild(max.Key()) == nil)

	lastChar = uint8(0)
	for char := 255; char >= 0; char-- {
		i, j := char>>6, char%64
		if node.presents[i]&(1<<j) != 0 {
			lastChar = uint8(char)
			break
		}
	}

	max = (*node.LastChild())
	assert.True(t, reflect.DeepEqual(max, node.children[lastChar]))
}
