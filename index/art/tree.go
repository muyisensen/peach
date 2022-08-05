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

	cp, depth := t.root, 0
	for cp != nil {
		var (
			current = *cp
			cKey    = current.Key()
		)

		if current.Kind() == kindLeaf {
			if bytes.Equal(key[depth:], cKey) {
				return current.Value()
			}
			return
		}

		if !bytes.HasPrefix(key[depth:], cKey) {
			return
		}

		depth += len(cKey)
		cp = current.FindChild(key[depth:])
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

	cp, depth := t.root, 0
	for cp != nil {
		var (
			current = *cp
			cKey    = current.Key()
			lcpIdx  = longestCommonPrefix(key[depth:], cKey)
		)

		if current.Kind() == kindLeaf && bytes.Equal(cKey, key[depth:]) {
			replaced = current.Value()
			current.SetValue(value)
			return
		}

		if current.Kind() != kindLeaf && len(cKey) == lcpIdx {
			depth += lcpIdx
			if child := current.FindChild(key[depth:]); child != nil && *child != nil {
				cp = child
				continue
			}

			newNode := t.pool.Upgrade(current)
			*cp = newNode
			current = newNode

			current.InsertChild(t.pool.NewLeaf(key[depth:], value))
			t.size++
			return
		}

		current.SetKey(cKey[lcpIdx:])
		newNode := t.pool.Alloc(kindNode4)
		newNode.SetKey(cKey[:lcpIdx])
		newNode.InsertChild(current)
		newNode.InsertChild(t.pool.NewLeaf(key[depth+lcpIdx:], value))
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

	cp, depth := t.root, 0
	for cp != nil {
		var (
			current = *cp
			cKey    = current.Key()
		)

		if !bytes.HasPrefix(key[depth:], cKey) {
			return
		}

		depth += len(cKey)
		p := current.FindChild(key[depth:])
		if p == nil {
			return
		}
		child := *p

		if child.Kind() == kindLeaf {
			if bytes.Equal(key[depth:], child.Key()) {
				deleted = child.Value()
				t.pool.Recycle(current.RemoveChild(key[depth:]))
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
