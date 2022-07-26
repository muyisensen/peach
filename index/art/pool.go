package art

import (
	"github.com/muyisensen/peach/index"
	"github.com/muyisensen/peach/utils"
)

type (
	nodePool struct {
		opts        *index.AdaptiveRadixTreeOptions
		mapNodeList map[kind]*utils.LinkedList
	}
)

func newNodePool(opts *index.AdaptiveRadixTreeOptions) *nodePool {
	np := &nodePool{opts: opts, mapNodeList: map[kind]*utils.LinkedList{}}

	np.newNodeLeafList()
	np.newNode4List()
	np.newNode16List()
	np.newNode48List()
	np.newNode256List()

	return np
}

func (np *nodePool) Alloc(k kind) treeNode {
	list, ok := np.mapNodeList[k]
	if !ok || list == nil {
		return nil
	}

	size := np.poolSize(k)
	if list.Size() <= size/8 {
		for i := 0; i < size/4; i++ {
			list.Append(np.newNode(k))
		}
	}

	node, ok := list.RemoveHead().(treeNode)
	if !ok {
		return nil
	}
	return node
}

func (np *nodePool) NewLeaf(key []byte, value *index.MemValue) treeNode {
	leaf := np.Alloc(kindLeaf)
	leaf.SetKey(key)
	leaf.SetValue(value)
	return leaf
}

func (np *nodePool) Recycle(no treeNode) {
	if isNil(no) {
		return
	}

	list, ok := np.mapNodeList[no.Kind()]
	if !ok {
		return
	}

	list.Append(np.clean(no))
	if list.Size() >= np.poolSize(no.Kind())*4 {
		list.Truncate(list.Size() / 2)
	}
}

func (np *nodePool) Upgrade(no treeNode) treeNode {
	if isNil(no) {
		return no
	}

	k := no.Kind()
	if no.NumOfChild()+1 <= np.maxNodeSize(k) {
		return no
	}

	switch k {
	case kindNode4:
		return np.upgradeNode16(no.(*node4))
	case kindNode16:
		return np.upgradeNode48(no.(*node16))
	case kindNode48:
		return np.upgradeNode256(no.(*node48))
	default:
		return no
	}
}

func (np *nodePool) Downgrade(no treeNode) treeNode {
	if isNil(no) {
		return no
	}

	k := no.Kind()
	if no.NumOfChild() >= np.minNodeSize(k) {
		return no
	}

	switch k {
	case kindNode256:
		return np.downgradeNode48(no.(*node256))
	case kindNode48:
		return np.downgradeNode16(no.(*node48))
	case kindNode16:
		return np.downgradeNode4(no.(*node16))
	case kindNode4:
		return np.pathCompression(no.(*node4))
	default:
		return no
	}
}

func (np *nodePool) newNodeLeafList() {
	list, ok := np.mapNodeList[kindLeaf]
	if !ok || list == nil {
		list = utils.NewLinkedList()
		np.mapNodeList[kindLeaf] = list
	}
	for i := 0; i < np.opts.NodeLeafPoolSize; i++ {
		list.Append(&nodeLeaf{})
	}
}

func (np *nodePool) newNode4List() {
	list, ok := np.mapNodeList[kindNode4]
	if !ok || list == nil {
		list = utils.NewLinkedList()
		np.mapNodeList[kindNode4] = list
	}
	for i := 0; i < np.opts.Node4PoolSize; i++ {
		list.Append(&node4{})
	}
}

func (np *nodePool) newNode16List() {
	list, ok := np.mapNodeList[kindNode16]
	if !ok || list == nil {
		list = utils.NewLinkedList()
		np.mapNodeList[kindNode16] = list
	}
	for i := 0; i < np.opts.Node16PoolSize; i++ {
		np.mapNodeList[kindNode16].Append(&node16{})
	}
}

func (np *nodePool) newNode48List() {
	list, ok := np.mapNodeList[kindNode48]
	if !ok || list == nil {
		list = utils.NewLinkedList()
		np.mapNodeList[kindNode48] = list
	}
	for i := 0; i < np.opts.Node48PoolSize; i++ {
		np.mapNodeList[kindNode48].Append(&node48{})
	}
}

func (np *nodePool) newNode256List() {
	list, ok := np.mapNodeList[kindNode256]
	if !ok || list == nil {
		list = utils.NewLinkedList()
		np.mapNodeList[kindNode256] = list
	}
	for i := 0; i < np.opts.Node256PoolSize; i++ {
		np.mapNodeList[kindNode256].Append(&node256{})
	}
}

func (np *nodePool) poolSize(k kind) int {
	switch k {
	case kindLeaf:
		return np.opts.NodeLeafPoolSize
	case kindNode4:
		return np.opts.Node4PoolSize
	case kindNode16:
		return np.opts.Node16PoolSize
	case kindNode48:
		return np.opts.Node48PoolSize
	case kindNode256:
		return np.opts.Node256PoolSize
	default:
		return 0
	}
}

func (np *nodePool) newNode(k kind) treeNode {
	switch k {
	case kindLeaf:
		return &nodeLeaf{}
	case kindNode4:
		return &node4{}
	case kindNode16:
		return &node16{}
	case kindNode48:
		return &node48{}
	case kindNode256:
		return &node256{}
	default:
		return nil
	}
}

func (np *nodePool) clean(no treeNode) treeNode {
	switch no.Kind() {
	case kindLeaf:
		return np.cleanNodeLeaf(no.(*nodeLeaf))
	case kindNode4:
		return np.cleanNode4(no.(*node4))
	case kindNode16:
		return np.cleanNode16(no.(*node16))
	case kindNode48:
		return np.cleanNode48(no.(*node48))
	case kindNode256:
		return np.cleanNode256(no.(*node256))
	default:
		return nil
	}
}

func (np *nodePool) cleanNodeLeaf(no *nodeLeaf) *nodeLeaf {
	no.key = nil
	no.value = nil
	return no
}

func (np *nodePool) cleanNode4(no *node4) *node4 {
	no.prefix = nil
	no.numOfChild = 0
	no.zeroLeaf = nil

	for i := 0; i < node4Max; i++ {
		no.keys[i] = 0
		no.children[i] = nil
	}

	return no
}

func (np *nodePool) cleanNode16(no *node16) *node16 {
	no.prefix = nil
	no.numOfChild = 0
	no.zeroLeaf = nil

	for i := 0; i < node16Max; i++ {
		no.keys[i] = 0
		no.children[i] = nil
	}

	return no
}

func (np *nodePool) cleanNode48(no *node48) *node48 {
	no.prefix = nil
	no.numOfChild = 0
	no.zeroLeaf = nil

	for i := 0; i < 256; i++ {
		no.keys[i] = 0
	}

	for i := 0; i < 4; i++ {
		no.presents[i] = 0
	}

	for i := 0; i < node48Max; i++ {
		no.children[i] = nil
	}

	return no
}

func (np *nodePool) cleanNode256(no *node256) *node256 {
	no.prefix = nil
	no.numOfChild = 0
	no.zeroLeaf = nil

	for i := 0; i < 4; i++ {
		no.presents[i] = 0
	}

	for i := 0; i < node256Max; i++ {
		no.children[i] = nil
	}

	return no
}

func (np *nodePool) maxNodeSize(k kind) int {
	switch k {
	case kindNode4:
		return node4Max
	case kindNode16:
		return node16Max
	case kindNode48:
		return node48Max
	case kindNode256:
		return node256Max
	default:
		return 0
	}
}

func (np *nodePool) minNodeSize(k kind) int {
	switch k {
	case kindNode4:
		return node4Min
	case kindNode16:
		return node16Min
	case kindNode48:
		return node48Min
	case kindNode256:
		return node256Min
	default:
		return 0
	}
}

func (np *nodePool) upgradeNode16(old *node4) *node16 {
	newNode := np.Alloc(kindNode16).(*node16)
	newNode.prefix = old.prefix
	newNode.zeroLeaf = old.zeroLeaf
	newNode.numOfChild = old.numOfChild

	for i := 0; i < int(old.numOfChild); i++ {
		newNode.keys[i] = old.keys[i]
		newNode.children[i] = old.children[i]
	}
	np.Recycle(old)

	return newNode
}

func (np *nodePool) upgradeNode48(old *node16) *node48 {
	newNode := np.Alloc(kindNode48).(*node48)
	newNode.prefix = old.prefix
	newNode.zeroLeaf = old.zeroLeaf
	newNode.numOfChild = old.numOfChild

	for k := 0; k < int(old.numOfChild); k++ {
		c := old.keys[k]
		i, j := c>>6, c%64

		newNode.presents[i] |= (1 << j)
		newNode.keys[c] = uint8(k)
		newNode.children[k] = old.children[k]
	}
	np.Recycle(old)

	return newNode
}

func (np *nodePool) upgradeNode256(old *node48) *node256 {
	newNode := np.Alloc(kindNode256).(*node256)
	newNode.prefix = old.prefix
	newNode.zeroLeaf = old.zeroLeaf
	newNode.numOfChild = uint16(old.numOfChild)
	newNode.presents = old.presents

	for c := 0; c < 256; c++ {
		i, j := c>>6, c%64
		if old.presents[i]&(1<<j) != 0 {
			newNode.children[c] = old.children[old.keys[c]]
		}
	}
	np.Recycle(old)

	return newNode
}

func (np *nodePool) downgradeNode48(old *node256) *node48 {
	newNode := np.Alloc(kindNode48).(*node48)
	newNode.prefix = old.prefix
	newNode.zeroLeaf = old.zeroLeaf
	newNode.numOfChild = uint8(old.numOfChild)
	newNode.presents = old.presents

	k := uint8(0)
	for c := 0; c < 256; c++ {
		i, j := c>>6, c%64
		if old.presents[i]&(1<<j) != 0 {
			newNode.keys[c] = k
			newNode.children[k] = old.children[c]
			k++
		}
	}
	np.Recycle(old)

	return newNode
}

func (np *nodePool) downgradeNode16(old *node48) *node16 {
	newNode := np.Alloc(kindNode16).(*node16)
	newNode.prefix = old.prefix
	newNode.zeroLeaf = old.zeroLeaf
	newNode.numOfChild = old.numOfChild

	k := uint8(0)
	for c := 0; c < 256; c++ {
		i, j := c>>6, c%64
		if old.presents[i]&(1<<j) != 0 {
			newNode.keys[k] = uint8(c)
			newNode.children[k] = old.children[old.keys[c]]
			k++
		}
	}
	np.Recycle(old)

	return newNode
}

func (np *nodePool) downgradeNode4(old *node16) *node4 {
	newNode := np.Alloc(kindNode4).(*node4)
	newNode.prefix = old.prefix
	newNode.zeroLeaf = old.zeroLeaf
	newNode.numOfChild = old.numOfChild

	for i := 0; i < int(old.numOfChild); i++ {
		newNode.keys[i] = old.keys[i]
		newNode.children[i] = old.children[i]
	}
	np.Recycle(old)

	return newNode
}

func (np *nodePool) pathCompression(old *node4) treeNode {
	switch {
	case old.NumOfChild() == 0 && !isNil(old.zeroLeaf):
	case old.NumOfChild() == 1 && isNil(old.zeroLeaf):
	default:
		return old
	}

	child := old.zeroLeaf
	if isNil(child) {
		child = old.children[0]
	}
	old.RemoveChild(child.Key())

	newKey := make([]byte, 0, len(old.Key())+len(child.Key()))
	newKey = append(newKey, old.Key()...)
	newKey = append(newKey, child.Key()...)
	child.SetKey(newKey)
	np.Recycle(old)
	return child
}
