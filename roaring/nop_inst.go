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

// +build !btreeInstrumentation

package roaring

//lint:ignore U1000 this is conditional on a build flag
//nolint:unused
type dTree struct {
}

//lint:ignore U1000 this is conditional on a build flag
//nolint:unused
type treeInst struct {
}

func (t *tree) didCopy(n int) {
}

func (d *d) didCopy(n int) {
}

func (t *tree) countCopies() int64 {
	return 0
}

func (d *d) setTree(t *tree) {
}
