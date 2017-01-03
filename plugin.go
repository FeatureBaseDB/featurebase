package pilosa

import (
	"context"

	"github.com/pilosa/pilosa/pql"
)

// Plugin represents pluggable functionality into pilosa.
type Plugin interface {
	Map(ctx context.Context, db string, args []pql.Arg, slice uint64) (interface{}, error)
	Reduce(ctx context.Context, prev, v interface{}) interface{}
}

// PluginRegistry holds a lookup of plugins.
type PluginRegistry struct {
	fns map[string]NewPluginFunc
}

// NewPluginRegistry returns a new instance of PluginRegistry.
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{
		fns: make(map[string]NewPluginFunc),
	}
}

type NewPluginFunc func() Plugin
