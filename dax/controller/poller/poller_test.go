package poller_test

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	controllerhttp "github.com/featurebasedb/featurebase/v3/dax/controller/http"
	"github.com/featurebasedb/featurebase/v3/dax/controller/poller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

// TestPoller runs for a total of 5 seconds. It begins by polling two healthy
// nodes. After 3 seconds, one of the nodes dies. At that point, the poller
// de-registers the dead node from the node manager, after which the node
// manager tells the poller to stop polling the dead node.
func TestPoller(t *testing.T) {
	ctx := context.Background()

	workerRegistry := newMemWorkerRegistry()

	// node 1
	node1 := newMockNode(t, "health", 0)
	defer node1.Close()
	addr1 := dax.Address(node1.URL())
	daxNode1 := &dax.Node{
		Address: addr1,
	}

	// node 2
	node2 := newMockNode(t, "health", 3*time.Second)
	defer node2.Close()
	addr2 := dax.Address(node2.URL())
	daxNode2 := &dax.Node{
		Address: addr2,
	}

	// manager
	manager := newMockManager(t, ctx, "deregister-nodes", workerRegistry)
	defer manager.Close()
	managerAddr := dax.Address(manager.URL())

	t.Run("Poller", func(t *testing.T) {
		cfg := poller.Config{
			AddressManager: controllerhttp.NewAddressManager(managerAddr),
			NodePoller:     poller.NewHTTPNodePoller(logger.NopLogger),
			WorkerRegistry: workerRegistry,
		}
		p := poller.New(cfg)

		done := make(chan struct{})
		go func() {
			time.Sleep(5 * time.Second)
			close(done)
		}()

		// Add workers to workerRegistry so they are available to the poller.
		workerRegistry.AddWorker(ctx, addr1, daxNode1)
		workerRegistry.AddWorker(ctx, addr2, daxNode2)

		go p.Run()
		defer p.Stop()

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

	workerRegistry dax.WorkerRegistry
}

func newMockManager(t *testing.T, ctx context.Context, deregisterPath string, wr dax.WorkerRegistry) *mockManager {
	mm := &mockManager{
		t:              t,
		workerRegistry: wr,
	}

	// deregister is a function used in this mock to remove the address from the
	// addresses cache in the mock manager, as well as call RemoveAddresses on
	// the Poller.
	deregister := func(addrs ...dax.Address) {
		for _, addr := range addrs {
			mm.workerRegistry.RemoveWorker(context.Background(), addr)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/"+deregisterPath, r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))

		////// handle payload
		body := r.Body
		defer body.Close()

		req := controllerhttp.DeregisterNodesRequest{}
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

type memWorkerRegistry struct {
	mu        sync.RWMutex
	addresses map[dax.Address]*dax.Node
}

func newMemWorkerRegistry() *memWorkerRegistry {
	return &memWorkerRegistry{
		addresses: make(map[dax.Address]*dax.Node),
	}
}

func (m *memWorkerRegistry) AddWorker(ctx context.Context, addr dax.Address, node *dax.Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addresses[addr] = node
	return nil
}

func (m *memWorkerRegistry) Worker(ctx context.Context, addr dax.Address) (*dax.Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.addresses[addr]
	if !ok {
		return nil, errors.Errorf("node does not exist")
	}
	return node, nil
}

func (m *memWorkerRegistry) RemoveWorker(ctx context.Context, addr dax.Address) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.addresses, addr)
	return nil
}

func (m *memWorkerRegistry) Workers(ctx context.Context) ([]*dax.Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*dax.Node, 0, len(m.addresses))
	for _, node := range m.addresses {
		nodes = append(nodes, node)
	}

	return nodes, nil
}
