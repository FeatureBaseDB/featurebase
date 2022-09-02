// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package storage

// public strings that pilosa/server/config.go can reference
const (
	RBFBackend string = "rbf"
)

// DefaultBackend is set here. pilosa/server/config.go references it
// to set the default for pilosa server exeutable.
const DefaultBackend = RBFBackend

// Config represents configuration which applies to multiple storage engines.
type Config struct {
	Backend string `toml:"backend"`

	// Set before calling db.Open()
	FsyncEnabled bool `toml:"fsync"`
}

// NewDefaultConfig returns a new Config with default values.
func NewDefaultConfig() *Config {
	return &Config{
		Backend:      DefaultBackend,
		FsyncEnabled: true,
	}
}
