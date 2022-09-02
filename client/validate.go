// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"regexp"
)

const (
	maxLabel = 64
	maxKey   = 64
)

var labelRegex = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_-]*$")
var keyRegex = regexp.MustCompile("^[A-Za-z0-9_{}+/=.~%:-]*$")

// ValidLabel returns true if the given label is valid, otherwise false.
func ValidLabel(label string) bool {
	return len(label) <= maxLabel && labelRegex.Match([]byte(label))
}

// ValidKey returns true if the given key is valid, otherwise false.
func ValidKey(key string) bool {
	return len(key) <= maxKey && keyRegex.Match([]byte(key))
}

func validateLabel(label string) error {
	if ValidLabel(label) {
		return nil
	}
	return ErrInvalidLabel
}

func validateKey(key string) error {
	if ValidKey(key) {
		return nil
	}
	return ErrInvalidKey
}
