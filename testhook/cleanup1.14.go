//go:build go1.14
// +build go1.14

package testhook

import (
	"testing"
)

// Cleanup in 1.13 logs a message about skipping a cleanup function, but
// allows things to build. Cleanup in 1.14 uses tb.Cleanup to register
// a cleanup function to call when a test completes.
func Cleanup(tb testing.TB, fn func()) {
	tb.Cleanup(fn)
}
