// Package poller provides the core Poller struct.
package poller

import (
	"context"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/logger"
)

// Poller maintains a list of nodes to poll. It also polls them.
type Poller struct {
	mu sync.RWMutex

	addresses map[dax.Address]struct{}

	addressManager dax.AddressManager

	nodePoller   NodePoller
	pollInterval time.Duration

	running  bool
	stopping chan struct{}

	logger logger.Logger
}

// New returns a new instance of Poller with default values.
func New(cfg Config) *Poller {
	p := &Poller{
		addresses:      make(map[dax.Address]struct{}),
		addressManager: dax.NewNopAddressManager(),
		nodePoller:     NewNopNodePoller(),
		pollInterval:   time.Second,
		stopping:       make(chan struct{}),
		logger:         logger.NopLogger,
	}

	// Set config options.
	if cfg.AddressManager != nil {
		p.addressManager = cfg.AddressManager
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

func (p *Poller) AddAddresses(ctx context.Context, addrs ...dax.Address) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, addr := range addrs {
		p.addresses[addr] = struct{}{}
	}

	return nil
}

func (p *Poller) RemoveAddresses(ctx context.Context, addrs ...dax.Address) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, addr := range addrs {
		delete(p.addresses, addr)
	}

	return nil
}

func (p *Poller) Addresses() []dax.Address {
	p.mu.RLock()
	defer p.mu.RUnlock()

	addrs := make([]dax.Address, 0, len(p.addresses))
	for addr := range p.addresses {
		addrs = append(addrs, addr)
	}

	return addrs
}

// Run starts the polling goroutine.
func (p *Poller) Run() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		p.logger.Printf("poller is already running")
		return
	}
	p.running = true

	go func() { p.run() }()
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
		p.logger.Debugf("polling:   %s", addr)
		start := time.Now()
		up := p.nodePoller.Poll(addr)
		if !up {
			p.logger.Printf("poller removing %s", addr)
			toRemove = append(toRemove, addr)
		}
		p.logger.Debugf("done poll: %s, %s", addr, time.Since(start))
	}

	if len(toRemove) > 0 {
		p.logger.Debugf("removing addresses: %v", toRemove)
		start := time.Now()
		err := p.addressManager.RemoveAddresses(ctx, toRemove...)
		if err != nil {
			p.logger.Printf("removing %s: %v", toRemove, err)
		}
		p.logger.Debugf("remove complete: %s", time.Since(start))
	}

}
