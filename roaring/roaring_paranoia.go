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

// +build roaringparanoia

package roaring

import "fmt"

const roaringParanoia = true

// CheckN verifies that the container's cached count is correct. Note
// that this has two definitions, depending on the presence of the
// roaringparanoia build tag.
func (c *Container) CheckN() {
	if c == nil {
		return
	}
	count := c.count()
	if count != c.n {
		panic(fmt.Sprintf("CheckN (%p): n %d, count %d", c, c.n, count))
	}
}
