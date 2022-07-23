package art

import (
	"bytes"

	"github.com/muyisensen/peach/index"
	"github.com/muyisensen/peach/utils"
)

type (
	iterator struct {
		t        *tree
		stack    *utils.SimpleStack
		nextLeaf treeNode
	}

	packet struct {
		node    treeNode
		visited bool
	}
)

var _ index.Iterator = &iterator{}

func newIterator(t *tree) *iterator {
	stack := &utils.SimpleStack{}
	stack.Push(&packet{node: *(t.root), visited: false})
	return &iterator{stack: stack, t: t}
}

func (i *iterator) HasNext() bool {
	i.t.mu.RLock()
	defer i.t.mu.RUnlock()

	i.nextLeaf = nil
	for i.stack.Size() > 0 {
		elem := i.stack.Pop()
		p, ok := elem.(*packet)
		if !ok {
			continue
		}

		if isNil(p.node) {
			continue
		}

		if p.node.Kind() == kindLeaf {
			i.nextLeaf = p.node
			break
		}

		if p.visited {
			continue
		}

		p.visited = true
		i.stack.Push(p)
		children := p.node.ListAllChild()
		for j := len(children) - 1; j >= 0; j-- {
			i.stack.Push(&packet{node: children[j], visited: false})
		}
	}

	return i.nextLeaf != nil
}

func (i *iterator) Next() (key []byte, value *index.MemValue) {
	i.t.mu.RLock()
	defer i.t.mu.RUnlock()

	if isNil(i.nextLeaf) {
		return
	}

	keys := make([][]byte, 0)
	for _, item := range i.stack.All() {
		elem := item
		p, ok := elem.(*packet)
		if !ok {
			continue
		}

		if isNil(p.node) {
			continue
		}

		if p.node.Kind() != kindLeaf && p.visited {
			keys = append(keys, p.node.Key())
		}
	}

	keys = append(keys, i.nextLeaf.Key())
	key, value = bytes.Join(keys, []byte{}), i.nextLeaf.Value()

	return
}
