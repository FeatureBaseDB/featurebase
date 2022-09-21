// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"log"
	"os"
	"sync"
	"testing"

	"github.com/molecula/featurebase/v3/testhook"
)

// glue lets us make a thing which isn't a testing.TB, but can be passed
// around to the functions that take testing.TB.

// DirCleaner represents the subset of the testing.TB interface
// we care about, allowing us to take objects which behave like
// that without importing all of testing to get them.
type DirCleaner interface {
	Helper()
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
	Fatal(...interface{})
	Logf(string, ...interface{})
	Name() string
	TempDir() string
	Cleanup(func())
	Skip(...interface{})
}

// Verify that a TB is a DirCleaner
var _ DirCleaner = testing.TB(nil)
var _ DirCleaner = &wholeTestRunWrapper{}

// wholeTestRun is a thing that's shaped a bit like testing.TB,
// but it can be used across all the tests, running its cleanup functions
// at the very end of the testing process. this lets us create clusters
// using the same code and logic we would for per-test things, except
// substituting this, and then have a single global post-test-hook run
// their cleanup.
type wholeTestRun struct {
	setup        sync.Once
	mu           sync.Mutex
	tempDirs     []string
	cleanupFuncs []func()
}

// wholeTestRunWrapper is a test-specific thing that can refer to the
// global shared state, but also forwards everything *except* TempDir,
// Cleanup, and Logf to the tb it's created with.
type wholeTestRunWrapper struct {
	testing.TB
}

// globalT is a system-wide wholeTestRun. the first time it's used, for any
// reason, it registers a cleanup with testhook, which will run at the end
// of the TestMain stuff, but before any previously-registered cleanup
// functions (such as the auditor stuff) so it can ensure that everything's
// been deleted. Basically this exists to let us call `tb.TempDir` on
// things and get a directory which outlives the current test.
var globalT wholeTestRun

// NewWholeTestRun produces a wholeTestRunWrapper around TB, which overrides
// a couple of the TB's methods to get whole-test-friendly behaviors.
func NewWholeTestRun(tb testing.TB) *wholeTestRunWrapper {
	return &wholeTestRunWrapper{TB: tb}
}

func (w *wholeTestRun) Teardown() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, fn := range w.cleanupFuncs {
		fn()
	}
	for _, path := range w.tempDirs {
		// disregard errors because we don't care that much about them;
		// we assume RemoveAll probably works unless something's wrong.
		// see the comments on the testing package's internal removeAll,
		// which suggests the errors only happen on Windows, which we
		// don't support.
		_ = os.RemoveAll(path)
	}
}

func (w *wholeTestRun) Setup() {
	w.setup.Do(func() {
		testhook.RegisterPostTestHook(func() error {
			w.Teardown()
			return nil
		})
	})
}

// We provide a Logf here because the wholeTestRunWrapper could
// be set as a tb-replacement for something which lasts past the
// test that created it. That shouldn't happen, probably.
func (w *wholeTestRunWrapper) Logf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func (w *wholeTestRun) TempDir(tb DirCleaner) string {
	w.Setup()
	w.mu.Lock()
	defer w.mu.Unlock()
	path, err := os.MkdirTemp("", "test-temp-")
	if err != nil {
		tb.Fatalf("creating temp dir: %v", err)
	}
	globalT.tempDirs = append(globalT.tempDirs, path)
	return path
}

func (w *wholeTestRun) Cleanup(fn func()) {
	w.Setup()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cleanupFuncs = append(w.cleanupFuncs, fn)
}

func (w *wholeTestRunWrapper) TempDir() string {
	return globalT.TempDir(w.TB)
}

func (w *wholeTestRunWrapper) Cleanup(fn func()) {
	globalT.Cleanup(fn)
}
