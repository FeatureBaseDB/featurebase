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

//go:build !roaringparanoia
// +build !roaringparanoia

package roaring

const roaringParanoia = false

// CheckN verifies that a container's cached count is correct, but
// there are two versions; this is the one which doesn't actually
// do anything, because the check is expensive. Which one you get is
// controlled by the roaringparanoia build tag.
func (c *Container) CheckN() {
}
