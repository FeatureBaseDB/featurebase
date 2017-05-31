package pilosa

import (
	"context"
	"errors"
	"sync"
)

type NewPluginConstructor func(*Holder) Plugin
type Plugin interface {
	Map(ctx context.Context, db string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error)
	Reduce(ctx context.Context, prev, v interface{}) interface{}
}

// PluginRegistry holds a lookup of plugin constructors.
type pluginRegistry struct {
	mutex *sync.RWMutex
	fns   map[string]NewPluginConstructor
}

// newPluginRegistry returns a new instance of PluginRegistry.
func newPluginRegistry() *pluginRegistry {
	return &pluginRegistry{
		mutex: &sync.RWMutex{},
		fns:   make(map[string]NewPluginConstructor),
	}
}

var (
	pr = newPluginRegistry()
)

// Register registers a plugin constructor with the registry.
// Returns an error if the plugin is already registered.
func RegisterPlugin(name string, fn NewPluginConstructor) error {
	return pr.register(name, fn)
}

func (r *pluginRegistry) register(name string, fn NewPluginConstructor) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.fns[name] != nil {
		return errors.New("plugin already registered")
	}
	r.fns[name] = fn
	return nil
}

// NewPlugin instantiates an already loaded plugin.
func NewPlugin(name string, h *Holder) (Plugin, error) {
	return pr.newPlugin(name, h)
}

func (r *pluginRegistry) newPlugin(name string, h *Holder) (Plugin, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	fn := r.fns[name]
	if fn == nil {
		return nil, errors.New("plugin not found")
	}

	return fn(h), nil
}
