package art

import (
	"bytes"

	"github.com/muyisensen/peach/index"
)

type (
	tree struct {
		root *treeNode
		pool *nodePool
		size int64
	}
)

func NewAdaptiveRadixTree(opts *index.AdaptiveRadixTreeOptions) index.MemTable {
	return &tree{
		pool: newNodePool(opts),
	}
}

func (t *tree) Get(key []byte) (value *index.MemValue) {
	if t.root == nil || len(key) == 0 {
		return nil
	}

	cp := t.root
	for cp != nil {
		var (
			current = *cp
			cKey    = current.Key()
		)

		if current.Kind() == kindLeaf {
			if bytes.Equal(key, cKey) {
				return current.Value()
			}
			return
		}

		if !bytes.HasPrefix(key, cKey) {
			return
		}

		key = key[len(cKey):]
		b := []byte{}
		if len(key) > 0 {
			b = append(b, key[0])
		}
		cp = current.FindChild(b)
	}

	return
}

func (t *tree) Put(key []byte, value *index.MemValue) (replaced *index.MemValue) {
	if len(key) == 0 || value == nil {
		return
	}

	if t.root == nil {
		newNode := t.pool.NewLeaf(key, value)
		t.root = &newNode
		t.size++
		return
	}

	cp := t.root
	for cp != nil {
		var (
			current = *cp
			cKey    = current.Key()
			lcp     = largeCommonPerfix(key, cKey)
		)

		if current.Kind() == kindLeaf && bytes.Equal(cKey, key) {
			replaced = current.Value()
			current.SetValue(value)
			return
		}

		if current.Kind() != kindLeaf && len(cKey) == len(lcp) {
			key = key[len(lcp):]
			b := []byte{}
			if len(key) > 0 {
				b = append(b, key[0])
			}

			if child := current.FindChild(b); child != nil && *child != nil {
				cp = child
				continue
			}

			newNode := t.pool.Upgrade(current)
			*cp = newNode
			current = newNode

			current.InsertChild(t.pool.NewLeaf(key, value))
			t.size++
			return
		}

		current.SetKey(cKey[len(lcp):])
		newNode := t.pool.Alloc(kindNode4)
		newNode.SetKey(lcp)
		newNode.InsertChild(current)
		newNode.InsertChild(t.pool.NewLeaf(key[len(lcp):], value))
		*cp = newNode
		t.size++
		return
	}

	return
}

func (t *tree) Delete(key []byte) (deleted *index.MemValue) {
	if t.root == nil || len(key) == 0 {
		return nil
	}

	if no := *t.root; no.Kind() == kindLeaf {
		if bytes.Equal(key, no.Key()) {
			deleted = no.Value()
			t.pool.Recycle(no)
			t.root = nil
			t.size--
			return
		}
		return
	}

	cp := t.root
	for cp != nil {
		var (
			current = *cp
			cKey    = current.Key()
		)

		if !bytes.HasPrefix(key, cKey) {
			return
		}

		key = key[len(cKey):]
		b := []byte{}
		if len(key) > 0 {
			b = append(b, key[0])
		}

		p := current.FindChild(b)
		if p == nil {
			return
		}
		child := *p

		if child.Kind() == kindLeaf {
			if bytes.Equal(key, child.Key()) {
				deleted = child.Value()
				t.pool.Recycle(current.RemoveChild(b))
				t.size--
				*cp = t.pool.Downgrade(current)
				return
			}
			return
		}

		cp = p
	}

	return
}

func (t *tree) Minimum() (key []byte, value *index.MemValue) {
	if t.root == nil {
		return
	}

	cp, keys := t.root, make([]byte, 0)
	for cp != nil {
		current := *cp

		if current.Kind() == kindLeaf {
			break
		}

		keys = append(keys, current.Key()...)
		cp = current.FirstChild()
	}

	if cp == nil {
		return
	}

	node := *cp
	keys = append(keys, node.Key()...)
	return keys, node.Value()
}

func (t *tree) Maximum() (key []byte, value *index.MemValue) {
	if t.root == nil {
		return
	}

	cp, keys := t.root, make([]byte, 0)
	for cp != nil {
		current := *cp

		if current.Kind() == kindLeaf {
			break
		}

		keys = append(keys, current.Key()...)
		cp = current.LastChild()
	}

	if cp == nil {
		return
	}

	node := *cp
	keys = append(keys, node.Key()...)
	return keys, node.Value()
}

func (t *tree) Iterate() index.Iterator {
	return newIterator(t)
}

func (t *tree) Size() int64 {
	return t.size
}
