package pilosa

import (
	"context"
	"errors"

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

// Register registers a plugin constructor with the registry.
// Returns an error if the plugin is already registered.
func (r *PluginRegistry) Register(name string, fn NewPluginFunc) error {
	if r.fns[name] != nil {
		return errors.New("plugin already registered")
	}
	r.fns[name] = fn
	return nil
}

// NewPlugin instantiates an already loaded plugin.
func (r *PluginRegistry) NewPlugin(name string) (Plugin, error) {
	fn := r.fns[name]
	if fn == nil {
		return nil, errors.New("plugin not found")
	}

	p := fn()
	return p, nil
}

type NewPluginFunc func() Plugin
