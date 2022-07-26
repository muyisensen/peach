package utils

type ListNode struct {
	value interface{}
	next  *ListNode
	prev  *ListNode
}

type LinkedList struct {
	size int
	head *ListNode
	tail *ListNode
}

func NewLinkedList() *LinkedList {
	head, tail := &ListNode{}, &ListNode{}
	head.next, tail.prev = tail, head
	return &LinkedList{head: head, tail: tail}
}

func (ll *LinkedList) Append(value interface{}) {
	node := &ListNode{value: value}
	node.next, node.prev = ll.tail, ll.tail.prev
	ll.tail.prev.next, ll.tail.prev = node, node
	ll.size++
}

func (ll *LinkedList) Head() interface{} {
	if ll.size == 0 || ll.head.next == ll.tail {
		return nil
	}

	return ll.head.next.value
}

func (ll *LinkedList) Tail() interface{} {
	if ll.size == 0 || ll.tail.prev == ll.head {
		return nil
	}

	return ll.tail.prev.value
}

func (ll *LinkedList) RemoveHead() interface{} {
	if ll.size == 0 || ll.head.next == ll.tail {
		return nil
	}

	node := ll.head.next
	ll.head.next, node.next.prev = node.next, ll.head
	ll.size--
	return node.value
}

func (ll *LinkedList) RemoveTail() interface{} {
	if ll.size == 0 || ll.tail.prev == ll.head {
		return nil
	}

	node := ll.tail.prev
	ll.tail.prev, node.prev.next = node.prev, ll.tail
	ll.size--
	return node.value
}

func (ll *LinkedList) Truncate(size int) {
	if ll.size == 0 || ll.head.next == ll.tail {
		return
	}

	if size < 0 || size > ll.size {
		return
	}

	curr := ll.head
	for i := 0; i < size; i++ {
		curr = curr.next
	}
	ll.tail.prev, curr.next = curr, ll.tail
	ll.size = size
}

func (ll *LinkedList) Size() int {
	return ll.size
}
