// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import "testing"

func TestValidateLabel(t *testing.T) {
	labels := []string{
		"a", "ab", "ab1", "d_e", "A", "Bc", "B1", "aB", "b-c",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, label := range labels {
		if validateLabel(label) != nil {
			t.Fatalf("Should be valid label: %s", label)
		}
	}
}

func TestValidateLabelInvalid(t *testing.T) {
	labels := []string{
		"", "1", "_", "-", "'", "^", "/", "\\", "*", "a:b", "valid?no", "yüce",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
	}
	for _, label := range labels {
		if validateLabel(label) == nil {
			t.Fatalf("Should be invalid label: %s", label)
		}
	}
}

func TestValidateKey(t *testing.T) {
	keys := []string{
		"", "1", "ab", "ab1", "b-c", "d_e", "pilosa.com",
		"bbf8d41c-7dba-40c4-94dc-94677b43bcf3",                 // UUID
		"{bbf8d41c-7dba-40c4-94dc-94677b43bcf3}",               // Windows GUID
		"https%3A//www.pilosa.com/about/%23contact",            // escaped URL
		"aHR0cHM6Ly93d3cucGlsb3NhLmNvbS9hYm91dC8jY29udGFjdA==", // base64
		"urn:isbn:1234567",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, key := range keys {
		if validateKey(key) != nil {
			t.Fatalf("Should be valid key: %s", key)
		}
	}
}

func TestValidateKeyInvalid(t *testing.T) {
	keys := []string{
		"\"", "'", "slice\\dice", "valid?no", "yüce", "*xyz", "with space", "<script>",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
	}
	for _, key := range keys {
		if validateKey(key) == nil {
			t.Fatalf("Should be invalid key: %s", key)
		}
	}
}
