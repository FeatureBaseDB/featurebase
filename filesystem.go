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

package pilosa

import (
	"fmt"
	"net/http"
)

// Ensure nopStaticFileSystem implements interface.
var _ StaticFileSystem = &nopStaticFileSystem{}

// StaticFileSystem represents an interface for a static WebUI.
type StaticFileSystem interface {
	New() (http.FileSystem, error)
}

func init() {
	NopStaticFileSystem = &nopStaticFileSystem{}
}

// NopStaticFileSystem represents a StaticFileSystem that returns an error if called.
var NopStaticFileSystem StaticFileSystem

type nopStaticFileSystem struct{}

// New is a no-op implementation of StaticFileSystem New method.
func (n *nopStaticFileSystem) New() (http.FileSystem, error) {
	return nil, fmt.Errorf("static file system not implemented")
}
