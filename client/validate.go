// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

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
