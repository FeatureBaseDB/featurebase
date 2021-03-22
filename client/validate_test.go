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
