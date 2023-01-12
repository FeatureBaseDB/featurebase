package bufferpool

import "errors"

// ClockReplacer implements a clock replacer algorithm
type ClockReplacer struct {
	cList     *circularList
	clockHand **circularListNode
}

// NewClockReplacer instantiates a new clock replacer
func NewClockReplacer(poolSize int) *ClockReplacer {
	cList := newCircularList(poolSize)
	return &ClockReplacer{cList, &cList.head}
}

// Victim removes the victim frame as defined by the replacement policy
func (c *ClockReplacer) Victim() (FrameID, error) {
	if c.cList.size == 0 {
		return FrameID(INVALID_PAGE), errors.New("no victims available")
	}
	var victimFrameID FrameID
	currentNode := (*c.clockHand)
	for {

		if currentNode.value.(bool) {
			currentNode.value = false
			c.clockHand = &currentNode.next
		} else {
			frameID := currentNode.key.(FrameID)
			victimFrameID = frameID
			c.clockHand = &currentNode.next
			c.cList.remove(currentNode.key)
			return victimFrameID, nil
		}
	}
}

// Unpin unpins a frame, indicating that it can now be victimized
func (c *ClockReplacer) Unpin(id FrameID) {
	if !c.cList.hasKey(id) {
		c.cList.insert(id, true)
		if c.cList.size == 1 {
			c.clockHand = &c.cList.head
		}
	}
}

// Pin pins a frame, indicating that it should not be victimized until it is unpinned
func (c *ClockReplacer) Pin(id FrameID) {
	node := c.cList.find(id)
	if node == nil {
		return
	}
	if (*c.clockHand) == node {
		c.clockHand = &(*c.clockHand).next
	}
	c.cList.remove(id)
}

// Size returns the size of the clock
func (c *ClockReplacer) Size() int {
	return c.cList.size
}
