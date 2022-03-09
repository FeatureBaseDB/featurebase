package controller

import (
	"context"
	"time"

	"github.com/molecula/featurebase/v3/dax"
)

// nodeRegistrationRoutine is a long-running goroutine that reads
// newly registered nodes from a channel and sends out
// new directives to rebalance among all the nodes.
//
// If the provided timeout is 0, this routine will register each node placed on
// the channel immediately.
//
// If the provided timeout is >0, this routine will batch the nodes until the
// timeout time has passed. This prevents (for the case when scaling up by >1
// node at a time) multiple directives being sent out serially as each node
// joins, and instead tries to handle all new nodes simultaneously.
func (c *Controller) nodeRegistrationRoutine(nodes chan *dax.Node, timeout time.Duration) error {
	if timeout > 0 {
		return c.nodeRegistrationDelayed(nodes, timeout)
	}
	return c.nodeRegistrationInstant(nodes)
}

func (c *Controller) nodeRegistrationInstant(nodes chan *dax.Node) error {
	for node := range nodes {
		err := c.RegisterNodes(context.Background(), node)
		if err != nil {
			c.logger.Errorf("Registering node: %v, encountered error: %v", node, err)
		}
	}
	return nil
}

func (c *Controller) nodeRegistrationDelayed(nodes chan *dax.Node, timeout time.Duration) error {
	batch := []*dax.Node{}
	c.logger.Printf("Running with batch registration timeout: %v", timeout)
	for {
		select {
		case <-c.stopping:
			return nil
		case node := <-nodes:
			c.logger.Debugf("adding node: %+v", node)
			batch = append(batch, node)
		case <-time.After(timeout):
			c.logger.Debugf("no new nodes in last %s, batch: %d", timeout, len(batch))
			if len(batch) > 0 {
				err := c.RegisterNodes(context.Background(), batch...)
				if err != nil {
					c.logger.Errorf("Registering nodes: %v, encountered error: %v", batch, err)
				}

				batch = batch[:0] // reset batch
			}
		}
	}
}
