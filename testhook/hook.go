// Copyright 2021 Molecula Corp. All rights reserved.
package testhook

import (
	"fmt"
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
	// ... but put it at the beginning of the list, so they're LIFO,
	// so test hook pairs nest cleanly.
	copy(postHooks[1:], postHooks)
	postHooks[0] = fn
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

	// This inner function lets us get a deferred run of our post-test
	// hooks which necessarily succeeds, but can trap an error even
	// from those and set the return value. You can't do this with a
	// direct os.Exit() wrapper because defers don't run after os.Exit.
	// Don't ask how many tries it took me to figure that out, I'll
	// just cry.
	func() {
		defer func() {
			mu.Lock()
			defer mu.Unlock()
			for _, fn := range postHooks {
				err := fn()
				if err != nil {
					fmt.Fprintf(os.Stderr, "post-hook failure: %v\n", err)
					ret = 1
				}
			}
		}()
		ret = m.Run()
	}()
	os.Exit(ret)
}

// TempDir creates a temp directory that will be automatically deleted when
// this test completes, using go1.14's [TB].Cleanup() if available.
func TempDir(tb testing.TB, pattern string) (path string, err error) {
	path, err = os.MkdirTemp("", pattern)
	if err == nil {
		Cleanup(tb, func() {
			os.RemoveAll(path)
		})
	}
	return path, err
}

// TempFile creates a temp file that will be automatically deleted when
// this test completes, using go1.14's [TB].Cleanup() if available.
func TempFile(tb testing.TB, pattern string) (file *os.File, err error) {
	file, err = os.CreateTemp("", pattern)
	if err == nil {
		path := file.Name()
		Cleanup(tb, func() {
			file.Close()
			os.Remove(path)
		})
	}
	return file, err
}

// TempDirInDir creates a temp directory that will be automatically deleted when
// this test completes, using go1.14's [TB].Cleanup(), but with a specified
// path instead of the default Go TMPDIR. Only some tests use this, which is
// possibly an error...
func TempDirInDir(tb testing.TB, dir string, pattern string) (path string, err error) {
	path, err = os.MkdirTemp(dir, pattern)
	if err == nil {
		Cleanup(tb, func() {
			os.RemoveAll(path)
		})
	}
	return path, err
}

// TempFileInDir creates a temp file that will be automatically deleted when
// this test completes, using go1.14's [TB].Cleanup(), but with a specified
// path instead of the default Go TMPDIR. Only some tests use this, which is
// possibly an error...
func TempFileInDir(tb testing.TB, dir string, pattern string) (file *os.File, err error) {
	file, err = os.CreateTemp(dir, pattern)
	if err == nil {
		path := file.Name()
		Cleanup(tb, func() {
			file.Close()
			os.Remove(path)
		})
	}
	return file, err
}
