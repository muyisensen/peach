package art

import (
	"reflect"
	"testing"

	"github.com/muyisensen/peach/index"
	"github.com/stretchr/testify/assert"
)

func TestAlloc(t *testing.T) {
	pool := newNodePool(&index.AdaptiveRadixTreeOptions{
		NodeLeafPoolSize: 8,
		Node4PoolSize:    8,
		Node16PoolSize:   8,
		Node48PoolSize:   8,
		Node256PoolSize:  8,
	})

	for _, k := range []kind{kindLeaf, kindNode4, kindNode16, kindNode48, kindNode256} {
		for i := 0; i < 8; i++ {
			assert.False(t, isNil(pool.Alloc(k)))
		}
		assert.Equal(t, 2, pool.mapNodeList[k].Size())
	}

	// unsupport node kind
	assert.True(t, isNil(pool.Alloc(-1)))
}

func TestRecycle(t *testing.T) {
	pool := newNodePool(&index.AdaptiveRadixTreeOptions{
		NodeLeafPoolSize: 1,
		Node4PoolSize:    1,
		Node16PoolSize:   1,
		Node48PoolSize:   1,
		Node256PoolSize:  1,
	})

	for _, k := range []kind{kindLeaf, kindNode4, kindNode16, kindNode48, kindNode256} {
		size := 2
		for i := 0; i < 3; i++ {
			pool.Recycle(pool.newNode(k))
			if i == 2 {
				assert.Equal(t, 2, pool.mapNodeList[k].Size())
			} else {
				assert.Equal(t, size, pool.mapNodeList[k].Size())
			}
			size++
		}
	}
}

func TestUpgrade(t *testing.T) {
	pool := newNodePool(&index.AdaptiveRadixTreeOptions{
		NodeLeafPoolSize: 8,
		Node4PoolSize:    8,
		Node16PoolSize:   8,
		Node48PoolSize:   8,
		Node256PoolSize:  8,
	})

	leaf := pool.NewLeaf(nil, nil)
	no := pool.Upgrade(leaf)
	assert.Equal(t, kindLeaf, no.Kind())

	n4 := pool.Alloc(kindNode4).(*node4)
	n4.SetKey([]byte("abc"))
	for i := 0; i < 4; i++ {
		n4.InsertChild(pool.NewLeaf([]byte{byte(i)}, nil))
	}
	n4.InsertChild(pool.NewLeaf(nil, nil))

	cloneN4 := copyNode4(n4)
	n16 := pool.Upgrade(n4).(*node16)
	assert.Equal(t, kindNode16, n16.Kind())
	assert.Equal(t, cloneN4.NumOfChild(), n16.NumOfChild())
	assert.True(t, reflect.DeepEqual(cloneN4.Key(), n16.Key()))
	assert.True(t, reflect.DeepEqual(cloneN4.zeroLeaf, n16.zeroLeaf))
	assert.True(t, reflect.DeepEqual(cloneN4.keys[:], n16.keys[:4]))
	assert.True(t, reflect.DeepEqual(cloneN4.children[:], n16.children[:4]))

	for i := 4; i < 16; i++ {
		n16.InsertChild(pool.NewLeaf([]byte{byte(i)}, nil))
	}
	cloneN16 := copyNode16(n16)
	n48 := pool.Upgrade(n16).(*node48)
	assert.Equal(t, kindNode48, n48.Kind())
	assert.Equal(t, cloneN16.NumOfChild(), n48.NumOfChild())
	assert.True(t, reflect.DeepEqual(cloneN16.Key(), n48.Key()))
	assert.True(t, reflect.DeepEqual(cloneN16.zeroLeaf, n48.zeroLeaf))
	for index, char := range cloneN16.keys {
		i, j := char>>6, char%64
		assert.True(t, n48.presents[i]&(1<<j) != 0)
		c1, c2 := cloneN16.children[index], n48.children[n48.keys[char]]
		assert.True(t, reflect.DeepEqual(c1, c2))
	}

	for i := 16; i < 48; i++ {
		n48.InsertChild(pool.NewLeaf([]byte{byte(i)}, nil))
	}
	cloneN48 := copyNode48(n48)
	n256 := pool.Upgrade(n48).(*node256)
	assert.Equal(t, kindNode256, n256.Kind())
	assert.Equal(t, cloneN48.NumOfChild(), n256.NumOfChild())
	assert.True(t, reflect.DeepEqual(cloneN48.Key(), n256.Key()))
	assert.True(t, reflect.DeepEqual(cloneN48.zeroLeaf, n256.zeroLeaf))
	assert.True(t, reflect.DeepEqual(cloneN48.presents, n256.presents))
	for char := 0; char < 256; char++ {
		i, j := char>>6, char%64
		if cloneN48.presents[i]&(1<<j) != 0 {
			assert.True(t, reflect.DeepEqual(cloneN48.children[cloneN48.keys[char]], n256.children[char]))
		}
	}
}

func TestDowngrade(t *testing.T) {
	pool := newNodePool(&index.AdaptiveRadixTreeOptions{
		NodeLeafPoolSize: 8,
		Node4PoolSize:    8,
		Node16PoolSize:   8,
		Node48PoolSize:   8,
		Node256PoolSize:  8,
	})

	n256 := pool.Alloc(kindNode256).(*node256)
	for char := 0; char < 47; char++ {
		n256.InsertChild(pool.NewLeaf([]byte{byte(char)}, nil))
	}
	cloneN256 := copyNode256(n256)
	n48 := pool.Downgrade(n256).(*node48)
	assert.Equal(t, kindNode48, n48.Kind())
	assert.Equal(t, cloneN256.NumOfChild(), n48.NumOfChild())
	assert.True(t, reflect.DeepEqual(cloneN256.Key(), n48.Key()))
	assert.True(t, reflect.DeepEqual(cloneN256.zeroLeaf, n48.zeroLeaf))
	assert.True(t, reflect.DeepEqual(cloneN256.presents, n48.presents))
	for char := 0; char < 256; char++ {
		i, j := char>>6, char%64
		if cloneN256.presents[i]&(1<<j) != 0 {
			assert.True(t, reflect.DeepEqual(cloneN256.children[char], n48.children[n48.keys[char]]))
		}
	}

	for i := 46; i >= 16; i-- {
		n48.RemoveChild([]byte{byte(i)})
	}
	cloneN48 := copyNode48(n48)
	n16 := pool.Downgrade(n48).(*node16)
	assert.Equal(t, kindNode16, n16.Kind())
	assert.Equal(t, cloneN48.NumOfChild(), n16.NumOfChild())
	assert.True(t, reflect.DeepEqual(cloneN48.Key(), n16.Key()))
	assert.True(t, reflect.DeepEqual(cloneN48.zeroLeaf, n16.zeroLeaf))
	for index, char := range n16.keys {
		i, j := char>>6, char%64
		assert.True(t, cloneN48.presents[i]&(1<<j) != 0)
		assert.True(t, reflect.DeepEqual(cloneN48.children[cloneN48.keys[char]], n16.children[index]))
	}

	for i := 15; i >= 4; i-- {
		n16.RemoveChild([]byte{byte(i)})
	}
	cloneN16 := copyNode16(n16)
	n4 := pool.Downgrade(n16).(*node4)
	assert.Equal(t, kindNode4, n4.Kind())
	assert.Equal(t, cloneN16.NumOfChild(), n4.NumOfChild())
	assert.True(t, reflect.DeepEqual(cloneN16.Key(), n4.Key()))
	assert.True(t, reflect.DeepEqual(cloneN16.zeroLeaf, n4.zeroLeaf))
	assert.True(t, reflect.DeepEqual(cloneN16.keys[:4], n4.keys[:]))
	assert.True(t, reflect.DeepEqual(cloneN16.children[:4], n4.children[:]))
}

func copyNode4(orgin *node4) *node4 {
	return &node4{
		keys:       orgin.keys,
		children:   orgin.children,
		numOfChild: orgin.numOfChild,
		base:       orgin.base,
	}
}

func copyNode16(orgin *node16) *node16 {
	return &node16{
		keys:       orgin.keys,
		children:   orgin.children,
		numOfChild: orgin.numOfChild,
		base:       orgin.base,
	}
}

func copyNode48(orgin *node48) *node48 {
	return &node48{
		keys:       orgin.keys,
		children:   orgin.children,
		numOfChild: orgin.numOfChild,
		presents:   orgin.presents,
		base:       orgin.base,
	}
}

func copyNode256(orgin *node256) *node256 {
	return &node256{
		children:   orgin.children,
		numOfChild: orgin.numOfChild,
		presents:   orgin.presents,
		base:       orgin.base,
	}
}
