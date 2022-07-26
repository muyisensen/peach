package art

import "github.com/muyisensen/peach/index"

type kind int

const (
	kindLeaf kind = iota + 1
	kindNode4
	kindNode16
	kindNode48
	kindNode256

	node4Max   = 4
	node4Min   = 2
	node16Max  = 16
	node16Min  = 5
	node48Max  = 48
	node48Min  = 17
	node256Max = 256
	node256Min = 49
)

type treeNode interface {
	Kind() kind
	Key() []byte
	SetKey(key []byte)

	Value() *index.MemValue
	SetValue(value *index.MemValue)

	NumOfChild() int
	FindChild(b []byte) *treeNode
	InsertChild(child treeNode) (success bool)
	RemoveChild(b []byte) (deleted treeNode)
	ListAllChild() (sorted []treeNode)
	FirstChild() *treeNode
	LastChild() *treeNode
}

type (
	nodeLeaf struct {
		key   []byte
		value *index.MemValue
	}

	base struct {
		prefix   []byte
		zeroLeaf treeNode
	}

	node4 struct {
		base
		numOfChild uint8
		keys       [node4Max]byte
		children   [node4Max]treeNode
	}

	node16 struct {
		base
		numOfChild uint8
		keys       [node16Max]byte
		children   [node16Max]treeNode
	}

	node48 struct {
		base
		numOfChild uint8
		keys       [256]uint8
		children   [node48Max]treeNode
		presents   [4]uint64
	}

	node256 struct {
		base
		numOfChild uint16
		children   [node256Max]treeNode
		presents   [4]uint64
	}
)

var _ treeNode = &nodeLeaf{}
var _ treeNode = &node4{}
var _ treeNode = &node16{}
var _ treeNode = &node48{}
var _ treeNode = &node256{}

func NewLeaf(key []byte, value *index.MemValue) treeNode {
	return &nodeLeaf{key: key, value: value}
}

func (l *nodeLeaf) Kind() kind {
	return kindLeaf
}

func (l *nodeLeaf) Key() []byte {
	return l.key
}

func (l *nodeLeaf) SetKey(key []byte) {
	l.key = key
}

func (l *nodeLeaf) Value() *index.MemValue {
	return l.value
}

func (l *nodeLeaf) SetValue(value *index.MemValue) {
	l.value = value

}

func (l *nodeLeaf) NumOfChild() int {
	return 0
}

func (l *nodeLeaf) FindChild(b []byte) *treeNode {
	return nil
}

func (l *nodeLeaf) InsertChild(child treeNode) (success bool) {
	return false
}

func (l *nodeLeaf) RemoveChild(b []byte) (deleted treeNode) {
	return nil
}

func (l *nodeLeaf) ListAllChild() (sorted []treeNode) {
	return nil
}

func (l *nodeLeaf) FirstChild() *treeNode {
	return nil
}

func (l *nodeLeaf) LastChild() *treeNode {
	return nil
}

func (no *node4) Kind() kind {
	return kindNode4
}

func (no *node4) Key() []byte {
	return no.prefix
}

func (no *node4) SetKey(key []byte) {
	no.prefix = key
}

func (no *node4) Value() *index.MemValue {
	return nil
}

func (no *node4) SetValue(value *index.MemValue) {
}

func (no *node4) NumOfChild() int {
	return int(no.numOfChild)
}

func (no *node4) FindChild(b []byte) *treeNode {
	if len(b) == 0 {
		return &(no.zeroLeaf)
	}

	char := b[0]
	for i := 0; i < int(no.numOfChild); i++ {
		if char == no.keys[i] {
			return &(no.children[i])
		}
	}
	return nil
}

func (no *node4) InsertChild(child treeNode) (success bool) {
	if isNil(child) {
		return false
	}

	childKey := child.Key()
	if len(childKey) > 0 && no.numOfChild+1 > node4Max {
		return false
	}

	if len(childKey) == 0 {
		no.zeroLeaf = child
		return true
	}

	i, char := 0, childKey[0]
	for ; i < int(no.numOfChild) && no.keys[i] <= char; i++ {
	}

	for j := int(no.numOfChild) - 1; j >= i; j-- {
		no.keys[j+1] = no.keys[j]
		no.children[j+1] = no.children[j]
	}
	no.keys[i] = char
	no.children[i] = child
	no.numOfChild++

	return true
}

func (no *node4) RemoveChild(b []byte) (deleted treeNode) {
	if len(b) == 0 {
		deleted = no.zeroLeaf
		no.zeroLeaf = nil
		return
	}

	i, char := 0, b[0]
	for ; i < int(no.numOfChild) && no.keys[i] != char; i++ {
	}

	if i == int(no.numOfChild) {
		return
	}
	deleted = no.children[i]

	if i == int(no.numOfChild)-1 {
		no.keys[i] = 0
		no.children[i] = nil
		no.numOfChild--
		return
	}

	for j := i; j < int(no.numOfChild)-1; j++ {
		no.keys[j] = no.keys[j+1]
		no.children[j] = no.children[j+1]
	}
	no.numOfChild--

	return
}

func (no *node4) ListAllChild() (sorted []treeNode) {
	if !isNil(no.zeroLeaf) {
		sorted = append(sorted, no.zeroLeaf)
	}
	sorted = append(sorted, no.children[:no.numOfChild]...)

	return
}

func (no *node4) FirstChild() *treeNode {
	if !isNil(no.zeroLeaf) {
		return &no.zeroLeaf
	}

	if no.numOfChild == 0 {
		return nil
	}

	return &no.children[0]
}

func (no *node4) LastChild() *treeNode {
	if no.numOfChild == 0 {
		if !isNil(no.zeroLeaf) {
			return &no.zeroLeaf
		}
		return nil
	}

	return &no.children[no.numOfChild-1]
}

func (no *node16) Kind() kind {
	return kindNode16
}

func (no *node16) Key() []byte {
	return no.prefix
}

func (no *node16) SetKey(key []byte) {
	no.prefix = key
}

func (no *node16) Value() *index.MemValue {
	return nil
}

func (no *node16) SetValue(value *index.MemValue) {
}

func (no *node16) NumOfChild() int {
	return int(no.numOfChild)
}

func (no *node16) FindChild(b []byte) *treeNode {
	if len(b) == 0 {
		return &(no.zeroLeaf)
	}

	char := b[0]
	for i := 0; i < int(no.numOfChild); i++ {
		if char == no.keys[i] {
			return &(no.children[i])
		}
	}
	return nil
}

func (no *node16) InsertChild(child treeNode) (success bool) {
	if isNil(child) {
		return false
	}

	childKey := child.Key()
	if len(childKey) > 0 && no.numOfChild+1 > node16Max {
		return false
	}

	if len(childKey) == 0 {
		no.zeroLeaf = child
		return true
	}

	i, char := 0, childKey[0]
	for ; i < int(no.numOfChild) && no.keys[i] <= char; i++ {
	}

	for j := int(no.numOfChild) - 1; j >= i; j-- {
		no.keys[j+1] = no.keys[j]
		no.children[j+1] = no.children[j]
	}
	no.keys[i] = char
	no.children[i] = child
	no.numOfChild++

	return true
}

func (no *node16) RemoveChild(b []byte) (deleted treeNode) {
	if len(b) == 0 {
		deleted = no.zeroLeaf
		no.zeroLeaf = nil
		return
	}

	i, char := 0, b[0]
	for ; i < int(no.numOfChild) && no.keys[i] != char; i++ {
	}

	if i == int(no.numOfChild) {
		return
	}
	deleted = no.children[i]

	if i == int(no.numOfChild)-1 {
		no.keys[i] = 0
		no.children[i] = nil
		no.numOfChild--
		return
	}

	for j := i; j < int(no.numOfChild)-1; j++ {
		no.keys[j] = no.keys[j+1]
		no.children[j] = no.children[j+1]
	}
	no.numOfChild--

	return
}

func (no *node16) ListAllChild() (sorted []treeNode) {
	if !isNil(no.zeroLeaf) {
		sorted = append(sorted, no.zeroLeaf)
	}
	sorted = append(sorted, no.children[:no.numOfChild]...)

	return
}

func (no *node16) FirstChild() *treeNode {
	if !isNil(no.zeroLeaf) {
		return &no.zeroLeaf
	}

	if no.numOfChild == 0 {
		return nil
	}

	return &no.children[0]
}

func (no *node16) LastChild() *treeNode {
	if no.numOfChild == 0 {
		if !isNil(no.zeroLeaf) {
			return &no.zeroLeaf
		}
		return nil
	}

	return &no.children[no.numOfChild-1]
}

func (no *node48) Kind() kind {
	return kindNode48
}

func (no *node48) Key() []byte {
	return no.prefix
}

func (no *node48) SetKey(key []byte) {
	no.prefix = key
}

func (no *node48) Value() *index.MemValue {
	return nil
}

func (no *node48) SetValue(value *index.MemValue) {
}

func (no *node48) NumOfChild() int {
	return int(no.numOfChild)
}

func (no *node48) FindChild(b []byte) *treeNode {
	if len(b) == 0 {
		return &(no.zeroLeaf)
	}

	char := b[0]
	i, j := char>>6, char%64
	if no.presents[i]&(1<<j) == 0 {
		return nil
	}

	if idx := no.keys[char]; idx < no.numOfChild {
		return &(no.children[idx])
	}
	return nil
}

func (no *node48) InsertChild(child treeNode) (success bool) {
	if isNil(child) {
		return false
	}

	childKey := child.Key()
	if len(childKey) > 0 && no.numOfChild+1 > node48Max {
		return false
	}

	if len(childKey) == 0 {
		no.zeroLeaf = child
		return true
	}

	char := childKey[0]
	i, j := char>>6, char%64
	no.presents[i] |= (1 << j)

	idx := uint8(0)
	for no.children[idx] != nil {
		idx++
	}

	no.keys[char] = idx
	no.children[idx] = child
	no.numOfChild++
	return true
}

func (no *node48) RemoveChild(b []byte) (deleted treeNode) {
	if len(b) == 0 {
		deleted = no.zeroLeaf
		no.zeroLeaf = nil
		return
	}

	char := b[0]
	i, j := char>>6, char%64
	if no.presents[i]&(1<<j) == 0 {
		return
	}

	deleted = no.children[no.keys[char]]
	no.children[no.keys[char]] = nil
	no.keys[char] = 0
	no.presents[i] &= ^(1 << j)
	no.numOfChild--

	return
}

func (no *node48) ListAllChild() (sorted []treeNode) {
	if !isNil(no.zeroLeaf) {
		sorted = append(sorted, no.zeroLeaf)
	}

	for c := 0; c < 256; c++ {
		i, j := c>>6, c%64
		if no.presents[i]&(1<<j) != 0 {
			sorted = append(sorted, no.children[no.keys[c]])
		}
	}

	return
}

func (no *node48) FirstChild() *treeNode {
	if !isNil(no.zeroLeaf) {
		return &no.zeroLeaf
	}

	if no.numOfChild > 0 {
		for c := 0; c < 256; c++ {
			i, j := c>>6, c%64
			if no.presents[i]&(1<<j) != 0 {
				return &no.children[no.keys[c]]
			}
		}
	}

	return nil
}

func (no *node48) LastChild() *treeNode {
	if no.numOfChild > 0 {
		for c := 255; c >= 0; c-- {
			i, j := c>>6, c%64
			if no.presents[i]&(1<<j) != 0 {
				return &no.children[no.keys[c]]
			}
		}
	}

	if !isNil(no.zeroLeaf) {
		return &no.zeroLeaf
	}

	return nil
}

func (no *node256) Kind() kind {
	return kindNode256
}

func (no *node256) Key() []byte {
	return no.prefix
}

func (no *node256) SetKey(key []byte) {
	no.prefix = key
}

func (no *node256) Value() *index.MemValue {
	return nil
}

func (no *node256) SetValue(value *index.MemValue) {
}

func (no *node256) NumOfChild() int {
	return int(no.numOfChild)
}

func (no *node256) FindChild(b []byte) *treeNode {
	if len(b) == 0 {
		return &(no.zeroLeaf)
	}

	char := b[0]
	i, j := char>>6, char%64
	if no.presents[i]&(1<<j) == 0 {
		return nil
	}

	return &(no.children[char])
}

func (no *node256) InsertChild(child treeNode) (success bool) {
	if isNil(child) {
		return false
	}

	childKey := child.Key()
	if len(childKey) == 0 {
		no.zeroLeaf = child
		return true
	}

	char := childKey[0]
	i, j := char>>6, char%64
	if no.presents[i]&(1<<j) == 0 {
		no.numOfChild++
		no.presents[i] |= (1 << j)
	}
	no.children[char] = child
	return true
}

func (no *node256) RemoveChild(b []byte) (deleted treeNode) {
	if len(b) == 0 {
		deleted = no.zeroLeaf
		no.zeroLeaf = nil
		return
	}

	char := b[0]
	i, j := char>>6, char%64
	if no.presents[i]&(1<<j) == 0 {
		return
	}

	deleted = no.children[char]
	no.children[char] = nil
	no.presents[i] &= ^(1 << j)
	no.numOfChild--

	return
}

func (no *node256) ListAllChild() (sorted []treeNode) {
	if !isNil(no.zeroLeaf) {
		sorted = append(sorted, no.zeroLeaf)
	}

	for c := 0; c < 256; c++ {
		i, j := c>>6, c%64
		if no.presents[i]&(1<<j) != 0 {
			sorted = append(sorted, no.children[c])
		}
	}

	return
}

func (no *node256) FirstChild() *treeNode {
	if !isNil(no.zeroLeaf) {
		return &no.zeroLeaf
	}

	for c := 0; c < 256; c++ {
		i, j := c>>6, c%64
		if no.presents[i]&(1<<j) != 0 {
			return &no.children[c]
		}
	}
	return nil
}

func (no *node256) LastChild() *treeNode {
	for c := 255; c >= 0; c-- {
		i, j := c>>6, c%64
		if no.presents[i]&(1<<j) != 0 {
			return &no.children[c]
		}
	}

	if !isNil(no.zeroLeaf) {
		return &no.zeroLeaf
	}

	return nil
}
