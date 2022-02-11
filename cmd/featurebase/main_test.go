//go:build testrunmain
// +build testrunmain

package main

import (
	"testing"
)

// Wrapper test for main function used to get code coverage for end2end tests
func TestRunMain(t *testing.T) {
	main()
}
