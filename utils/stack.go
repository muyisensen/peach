package utils

type SimpleStack struct {
	size  int
	elems []interface{}
}

func NewSimpleStack(cap int) *SimpleStack {
	return &SimpleStack{
		size:  0,
		elems: make([]interface{}, cap),
	}
}

func (ss *SimpleStack) Peek() interface{} {
	if ss.size == 0 {
		return nil
	}

	return ss.elems[ss.size-1]
}

func (ss *SimpleStack) Pop() interface{} {
	if ss.size <= 0 {
		return nil
	}
	elem := ss.elems[ss.size-1]
	ss.size--
	return elem
}

func (ss *SimpleStack) Push(elem interface{}) {
	if ss.size >= len(ss.elems) {
		elemsCap := len(ss.elems)
		if elemsCap == 0 {
			elemsCap = 1
		}

		newCap := 0
		if elemsCap >= 1024 {
			newCap = elemsCap + 100
		} else {
			newCap = elemsCap * 2
		}

		newElems := make([]interface{}, newCap)
		copy(newElems[:ss.size], ss.elems[:ss.size])
		ss.elems = newElems
	}
	ss.elems[ss.size] = elem
	ss.size++
}

func (ss *SimpleStack) Size() int {
	return ss.size
}

func (ss *SimpleStack) All() []interface{} {
	return ss.elems[:ss.size]
}
