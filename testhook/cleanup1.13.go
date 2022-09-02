// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
//go:build !go1.14
// +build !go1.14

package testhook

import (
	"sync"
	"testing"
)

var cleanupFuncs []func()
var cleanupMu sync.Mutex

func init() {
	RegisterPostTestHook(runCleanupFuncs)
}

func runCleanupFuncs() error {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	for _, fn := range cleanupFuncs {
		fn()
	}
	return nil
}

// Cleanup in 1.13 logs a message about skipping a cleanup function, but
// allows things to build. Cleanup in 1.14 uses tb.Cleanup to register
// a cleanup function to call when a test completes.
func Cleanup(tb testing.TB, fn func()) {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	cleanupFuncs = append(cleanupFuncs, fn)
}
