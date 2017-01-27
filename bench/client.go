package bench

import (
	"context"
	"errors"
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
	client      *pilosa.Client
	ClientType  string `json:"client-type"`
	ContentType string `json:"content-type"`
}

// Init for HasClient looks at the ClientType field and creates a pilosa client
// either using the first host in the list of hosts or based on the agent
// number mod len(hosts)
func (h *HasClient) Init(hosts []string, agentNum int) error {
	var err error
	switch h.ClientType {
	case "single":
		h.client, err = firstHostClient(hosts)
	case "round_robin":
		h.client, err = roundRobinClient(hosts, agentNum)
	default:
		err = fmt.Errorf("Unsupported ClientType: %v", h.ClientType)
	}
	if err != nil {
		return err
	}

	switch h.ContentType {
	case "protobuf":
		return nil
	case "pql":
		return nil
	default:
		return fmt.Errorf("Unsupported ContentType: %v", h.ContentType)

	}
}

func (h *HasClient) ExecuteQuery(contentType, db, query string, ctx context.Context) (interface{}, error) {
	if contentType == "protobuf" {
		return h.client.ExecuteQuery(ctx, db, query, true)
	} else if contentType == "pql" {
		return h.client.ExecutePQL(ctx, db, query)
	} else {
		return nil, errors.New("unsupport content type")
	}
}
