// Package poller provides the core Poller struct.
package poller

import (
	"context"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Poller maintains a list of nodes to poll. It also polls them.
type Poller struct {
	mu sync.RWMutex

	addressManager dax.AddressManager

	nodeService dax.NodeService

	nodePoller   NodePoller
	pollInterval time.Duration

	stopping chan struct{}

	logger logger.Logger
}

// New returns a new instance of Poller with default values.
func New(cfg Config) *Poller {
	p := &Poller{
		addressManager: dax.NewNopAddressManager(),
		nodeService:    dax.NewNopNodeService(),
		nodePoller:     NewNopNodePoller(),
		pollInterval:   time.Second,
		stopping:       make(chan struct{}),
		logger:         logger.NopLogger,
	}

	// Set config options.
	if cfg.AddressManager != nil {
		p.addressManager = cfg.AddressManager
	}
	if cfg.NodeService != nil {
		p.nodeService = cfg.NodeService
	}
	if cfg.NodePoller != nil {
		p.nodePoller = cfg.NodePoller
	}
	if cfg.PollInterval != 0 {
		p.pollInterval = cfg.PollInterval
	}
	if cfg.Logger != nil {
		p.logger = cfg.Logger
	}

	return p
}

func (p *Poller) Addresses() []dax.Address {
	nodes, err := p.nodeService.Nodes(context.Background())
	if err != nil {
		p.logger.Printf("POLLER: unable to get nodes from node service")
	}

	addrs := make([]dax.Address, 0, len(nodes))
	for _, node := range nodes {
		addrs = append(addrs, node.Address)
	}

	return addrs
}

// Run starts the polling goroutine.
func (p *Poller) Run() error {
	p.run()
	return nil
}

func (p *Poller) run() {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		// Wait for tick or a close.
		select {
		case <-p.stopping:
			return
		case <-ticker.C:
		}

		p.pollAll()
	}

}

// Stop stops the polling routine.
func (p *Poller) Stop() {
	close(p.stopping)
}

func (p *Poller) pollAll() {
	addrs := p.Addresses()

	ctx := context.Background()

	toRemove := []dax.Address{}

	for _, addr := range addrs {
		up := p.nodePoller.Poll(addr)
		if !up {
			p.logger.Printf("poller removing %s", addr)
			toRemove = append(toRemove, addr)
		}
	}

	if len(toRemove) > 0 {
		p.logger.Debugf("POLLER: removing addresses: %v", toRemove)
		start := time.Now()
		err := p.addressManager.RemoveAddresses(ctx, toRemove...)
		if err != nil {
			p.logger.Printf("POLLER: error removing %s: %v", toRemove, err)
		}
		p.logger.Debugf("POLLER removing %v complete: %s", toRemove, time.Since(start))
	}

}
