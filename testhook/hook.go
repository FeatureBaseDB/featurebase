// Copyright 2020 Pilosa Corp.
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

package testhook

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
)

// Callback denotes a function which can be run on a testing.T, or testing.B,
// which performs additional functions typically before or after tests.
type Callback func() error

var preHooks []Callback
var postHooks []Callback
var mu sync.Mutex

// RegisterPostTestHook registers a function to be called after tests
// are run. It should return a nil error if it's okay, and a non-nil
// error to cause a non-zero exit status.
func RegisterPostTestHook(fn Callback) {
	mu.Lock()
	defer mu.Unlock()
	postHooks = append(postHooks, fn)
}

// RegisterPreTestHook registers a function to be called after tests
// are run. It should return a nil error if it's okay, and a non-nil
// error to cause a non-zero exit status.
func RegisterPreTestHook(fn Callback) {
	mu.Lock()
	defer mu.Unlock()
	preHooks = append(preHooks, fn)
}

// RunTestsWithHooks is a suitable implementation for TestMain; you can
// just invoke this from your TestMain, passing in m, and it runs the tests
// and then runs any registered pre/post hooks. If the hooks themselves try
// to register hooks, you will deadlock. Don't do that.
func RunTestsWithHooks(m *testing.M) {
	var ret int
	mu.Lock()
	for _, fn := range preHooks {
		err := fn()
		if err != nil {
			fmt.Fprintf(os.Stderr, "pre-hook failure: %v\n", err)
			ret = 1
		}
	}
	mu.Unlock()
	if ret != 0 {
		fmt.Fprint(os.Stderr, "pre-hooks failed, aborting.\n")
		os.Exit(ret)
	}
	ret = m.Run()
	mu.Lock()
	defer mu.Unlock()
	for _, fn := range postHooks {
		err := fn()
		if err != nil {
			fmt.Fprintf(os.Stderr, "post-hook failure: %v\n", err)
			ret = 1
		}
	}
	os.Exit(ret)
}

// TempDir creates a temp directory that will be automatically deleted when
// this test completes, using go1.14's [TB].Cleanup() if available.
func TempDir(tb testing.TB, pattern string) (path string, err error) {
	path, err = ioutil.TempDir("", pattern)
	if err == nil {
		Cleanup(tb, func() {
			os.RemoveAll(path)
			fmt.Println("--- testhook:", path, tb.Name())
		})
	}
	return path, err
}

// TempDirInDir creates a temp directory that will be automatically deleted when
// this test completes, using go1.14's [TB].Cleanup(), but with a specified
// path instead of the default Go TMPDIR. Only some tests use this, which is
// possibly an error...
func TempDirInDir(tb testing.TB, dir string, pattern string) (path string, err error) {
	path, err = ioutil.TempDir(dir, pattern)
	if err == nil {
		Cleanup(tb, func() {
			os.RemoveAll(path)
		})
	}
	return path, err
}
