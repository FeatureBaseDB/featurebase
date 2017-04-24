// +build linux

package pilosa

import "plugin"

// PluginsSupported is set to true on platforms that support plugins.
const PluginsSupported = true

// Load reads a plugin file into the registry.
func (r *pluginRegistry) Load(path string) error {
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
	if err := fn.(func(PluginRegistry) error)(r); err != nil {
		return err
	}

	return nil
}
