package bench

import (
	"fmt"

	"github.com/pilosa/pilosa"
)

func firstHostClient(hosts []string) (*pilosa.Client, error) {
	client, err := pilosa.NewClient(hosts[0])
	if err != nil {
		return nil, err
	}
	return client, nil
}

func roundRobinClient(hosts []string, agentNum int) (*pilosa.Client, error) {
	clientNum := agentNum % len(hosts)
	return firstHostClient(hosts[clientNum:])
}

// HasClient provides a reusable component for Benchmark implementations which
// provides the Init method, a ClientType argument and a cli internal variable.
type HasClient struct {
	client     *pilosa.Client
	ClientType string `json:"client-type"`
}

// Init for HasClient looks at the ClientType field and creates a pilosa client
// either using the first host in the list of hosts or based on the agent
// number mod len(hosts)
func (h *HasClient) Init(hosts []string, agentNum int) error {
	var err error
	switch h.ClientType {
	case "single":
		h.client, err = firstHostClient(hosts)
		return err
	case "round_robin":
		h.client, err = roundRobinClient(hosts, agentNum)
		return err
	default:
		return fmt.Errorf("Unsupported ClientType: %v", h.ClientType)
	}
}
