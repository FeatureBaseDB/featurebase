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
	"io/ioutil"
)

// mustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func mustOpenIndex() *Index {
	path, err := ioutil.TempDir("", "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := NewIndex(path, "i")
	if err != nil {
		panic(err)
	}
	if err := index.Open(); err != nil {
		panic(err)
	}
	return index
}

// reopen closes the index and reopens it.
func (i *Index) reopen() error {
	if err := i.Close(); err != nil {
		return err
	}
	if err := i.Open(); err != nil {
		return err
	}
	return nil
}
