// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

// public strings that pilosa/server/config.go can reference
const (
	RoaringBackend string = "roaring"
	RBFBackend     string = "rbf"
	BoltBackend    string = "bolt"
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
		FsyncEnabled: false,
	}
}
