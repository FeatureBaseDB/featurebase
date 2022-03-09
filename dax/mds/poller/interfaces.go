package poller

import (
	"fmt"
	"net/http"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/logger"
)

// NodePoller is an interface to anything which has the ability to poll a
// node.
type NodePoller interface {
	Poll(dax.Address) bool
}

// Ensure type implements interface.
var _ NodePoller = (*NopNodePoller)(nil)
var _ NodePoller = (*HTTPNodePoller)(nil)

// NopNodePoller is a no-op implementation of the NodePoller interface.
type NopNodePoller struct{}

func NewNopNodePoller() *NopNodePoller {
	return &NopNodePoller{}
}

func (p *NopNodePoller) Poll(addr dax.Address) bool {
	return true
}

// HTTPNodePoller is an http implementation of the NodePoller interface.
type HTTPNodePoller struct {
	logger logger.Logger
	client *http.Client
}

func NewHTTPNodePoller(logger logger.Logger) *HTTPNodePoller {
	return &HTTPNodePoller{
		logger: logger,
		client: &http.Client{
			Timeout: time.Second, // short timeout for polling to detect issues quickly. /health endpoints should always respond fast.
		},
	}
}

func (p *HTTPNodePoller) Poll(addr dax.Address) bool {
	url := fmt.Sprintf("%s/health", addr.WithScheme("http"))

	if resp, err := p.client.Get(url); err != nil {
		p.logger.Printf("poll error: %s\n", err)
		return false
	} else if resp.StatusCode != http.StatusOK {
		return false
	}

	return true
}
