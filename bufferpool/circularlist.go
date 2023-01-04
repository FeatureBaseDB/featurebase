package bufferpool

import (
	"errors"
)

type circularListNode struct {
	key   interface{}
	value interface{}
	next  *circularListNode
	prev  *circularListNode
}

type circularList struct {
	head     *circularListNode
	tail     *circularListNode
	size     int
	capacity int
}

func newCircularList(maxSize int) *circularList {
	return &circularList{nil, nil, 0, maxSize}
}

func (c *circularList) find(key interface{}) *circularListNode {
	ptr := c.head
	for i := 0; i < c.size; i++ {
		if ptr.key == key {
			return ptr
		}
		ptr = ptr.next
	}
	return nil
}

func (c *circularList) hasKey(key interface{}) bool {
	return c.find(key) != nil
}

func (c *circularList) insert(key interface{}, value interface{}) error {
	if c.size == c.capacity {
		return errors.New("list is full")
	}
	newNode := &circularListNode{key, value, nil, nil}
	if c.size == 0 {
		newNode.next = newNode
		newNode.prev = newNode
		c.head = newNode
		c.tail = newNode
		c.size++
		return nil
	}

	node := c.find(key)
	if node != nil {
		node.value = value
		return nil
	}

	newNode.next = c.head
	newNode.prev = c.tail
	c.tail.next = newNode
	if c.head == c.tail {
		c.head.next = newNode
	}
	c.tail = newNode
	c.head.prev = c.tail

	c.size++
	return nil
}

func (c *circularList) remove(key interface{}) {
	node := c.find(key)
	if node == nil {
		return
	}
	if c.size == 1 {
		c.head = nil
		c.tail = nil
		c.size--
		return
	}
	if node == c.head {
		c.head = c.head.next
	}
	if node == c.tail {
		c.tail = c.tail.prev
	}
	node.next.prev = node.prev
	node.prev.next = node.next
	c.size--
}
