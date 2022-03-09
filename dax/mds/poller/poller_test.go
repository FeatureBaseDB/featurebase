package poller_test

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	mds_http "github.com/molecula/featurebase/v3/dax/mds/http"
	"github.com/molecula/featurebase/v3/dax/mds/poller"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

// TestPoller runs for a total of 5 seconds. It begins by polling two healthy
// nodes. After 3 seconds, one of the nodes dies. At that point, the poller
// de-registers the dead node from the node manager, after which the node
// manager tells the poller to stop polling the dead node.
func TestPoller(t *testing.T) {
	ctx := context.Background()

	// node 1
	node1 := newMockNode(t, "health", 0)
	defer node1.Close()
	addr1 := dax.Address(node1.URL())

	// node 1
	node2 := newMockNode(t, "health", 3*time.Second)
	defer node2.Close()
	addr2 := dax.Address(node2.URL())

	// manager
	manager := newMockManager(t, ctx, "deregister-nodes", []dax.Address{addr1, addr2})
	defer manager.Close()
	managerAddr := dax.Address(manager.URL())

	t.Run("Poller", func(t *testing.T) {
		cfg := poller.Config{
			AddressManager: mds_http.NewAddressManager(managerAddr),
			NodePoller:     poller.NewHTTPNodePoller(logger.NopLogger),
		}
		p := poller.New(cfg)

		// This is a little strange, but basically we need the manager to be
		// able to call poller.RemoveAddresses, and since this test poller isn't
		// running as an http server (unlike everything else in this test:
		// manager, nodes), we give the manager a pointer to the Poller here so
		// it can call the RemoveAddresses method directly.
		manager.setPoller(p)

		done := make(chan struct{})
		go func() {
			time.Sleep(5 * time.Second)
			close(done)
		}()

		p.Run()
		defer p.Stop()

		p.AddAddresses(ctx, addr1, addr2)

		// wait for a done
		<-done

		assert.Contains(t, p.Addresses(), addr1)
		assert.NotContains(t, p.Addresses(), addr2)
	})
}

///////////////////////////////////////////////////////////////

type mockManager struct {
	t      *testing.T
	server *httptest.Server

	poller    *poller.Poller
	addresses map[dax.Address]struct{}
}

func newMockManager(t *testing.T, ctx context.Context, deregisterPath string, addrs []dax.Address) *mockManager {
	addresses := make(map[dax.Address]struct{})
	for _, addr := range addrs {
		addresses[addr] = struct{}{}
	}

	mm := &mockManager{
		t:         t,
		addresses: addresses,
	}

	// deregister is a function used in this mock to remove the address from the
	// addresses cache in the mock manager, as well as call RemoveAddresses on
	// the Poller.
	deregister := func(addrs ...dax.Address) {
		for _, addr := range addrs {
			delete(mm.addresses, addr)
		}
		mm.poller.RemoveAddresses(ctx, addrs...)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/"+deregisterPath, r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))

		////// handle payload
		body := r.Body
		defer body.Close()

		req := mds_http.DeregisterNodesRequest{}
		if err := json.NewDecoder(body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		log.Printf("deregister addresses: %s", req.Addresses)
		deregister(req.Addresses...)
		//////

		w.WriteHeader(http.StatusOK)
	}))

	mm.server = server
	return mm
}

func (m *mockManager) setPoller(p *poller.Poller) {
	m.poller = p
}

func (m *mockManager) URL() string {
	if m.server != nil {
		return m.server.URL
	}
	return ""
}

func (m *mockManager) Close() {
	if m.server != nil {
		m.server.Close()
	}
}

type mockNode struct {
	t      *testing.T
	server *httptest.Server
}

func newMockNode(t *testing.T, healthPath string, dieAfter time.Duration) *mockNode {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/"+healthPath, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	if dieAfter > 0 {
		go func() {
			time.Sleep(dieAfter)
			log.Printf("stopping node: %s", server.URL)
			server.Close()
		}()
	}
	return &mockNode{
		t:      t,
		server: server,
	}
}

func (m *mockNode) URL() string {
	if m.server != nil {
		return m.server.URL
	}
	return ""
}

func (m *mockNode) Close() {
	if m.server != nil {
		m.server.Close()
	}
}
