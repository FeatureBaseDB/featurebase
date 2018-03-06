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

package filesystem

import (
	"net/http"

	"github.com/pilosa/pilosa"
	"github.com/rakyll/statik/fs"
)

// Ensure nopStaticFileSystem implements interface.
var _ pilosa.StaticFileSystem = &StatikFS{}

type StatikFS struct{}

// New is a statik implementation of StaticFileSystem New method.
func (s *StatikFS) New() (http.FileSystem, error) {
	return fs.New()
}
