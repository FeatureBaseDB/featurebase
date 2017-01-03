// +build linux

package pilosa

import (
	"errors"
	"plugin"
)

// PluginsSupported is set to true on platforms that support plugins.
const PluginsSupported = true

// Register registers a plugin constructor with the registry.
// Returns an error if the plugin is already registered.
func (r *PluginRegistry) Register(name string, fn NewPluginFunc) error {
	if r.fns[name] != nil {
		return errors.New("plugin already registered")
	}
	r.fns[name] = fn
	return nil
}

// Load reads a plugin file into the registry.
func (r *PluginRegistry) Load(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return err
	}

	// Find registry function within shared object.
	fn, err := p.Lookup("Init")
	if err != nil {
		return err
	}

	// Execute registration.
	if err := fn.(func(*PluginRegistry) error)(r); err != nil {
		return err
	}

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
