package utils

type SimpleStack struct {
	elems []interface{}
}

func (ss *SimpleStack) Peek() interface{} {
	if len(ss.elems) == 0 {
		return nil
	}

	return ss.elems[len(ss.elems)-1]
}

func (ss *SimpleStack) Pop() interface{} {
	if len(ss.elems) == 0 {
		return nil
	}
	elem := ss.elems[len(ss.elems)-1]
	ss.elems = ss.elems[:len(ss.elems)-1]
	return elem
}

func (ss *SimpleStack) Push(elem interface{}) {
	ss.elems = append(ss.elems, elem)
}

func (ss *SimpleStack) Size() int {
	return len(ss.elems)
}

func (ss *SimpleStack) All() []interface{} {
	return ss.elems
}
