// +build !linux

package pilosa

import "errors"

// PluginsSupported is set to true on platforms that support plugins.
const PluginsSupported = false

// Load reads a plugin file into the registry.
func (r *PluginRegistry) Load(path string) error {
	return errors.New("plugins not supported")
}
