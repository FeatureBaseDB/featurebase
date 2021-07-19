// Copyright 2017 Pilosa Corp.
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

package pilosa_test

import (
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2"
	_ "github.com/molecula/featurebase/v2/test"
)

func TestAddressWithDefaults(t *testing.T) {
	tests := []struct {
		addr     string
		expected string
		err      string
	}{
		{addr: "", expected: "localhost:10101"},
		{addr: ":", expected: "localhost:10101"},
		{addr: "localhost", expected: "localhost:10101"},
		{addr: "localhost:", expected: "localhost:10101"},
		{addr: "127.0.0.1:10101", expected: "127.0.0.1:10101"},
		{addr: "127.0.0.1:", expected: "127.0.0.1:10101"},
		{addr: ":10101", expected: "localhost:10101"},
		{addr: ":55555", expected: "localhost:55555"},
		{addr: "1.2.3.4", expected: "1.2.3.4:10101"},
		{addr: "1.2.3.4:", expected: "1.2.3.4:10101"},
		{addr: "1.2.3.4:55555", expected: "1.2.3.4:55555"},
		// The following tests check the error conditions.
		{addr: "[invalid][addr]:port", err: pilosa.ErrInvalidAddress.Error()},
	}
	for _, test := range tests {
		actual, err := pilosa.AddressWithDefaults(test.addr)
		if err != nil {
			if !strings.Contains(err.Error(), test.err) {
				t.Errorf("expected error: %v, but got: %v", test.err, err)
			}
		} else {
			if actual.HostPort() != test.expected {
				t.Errorf("expected: %v, but got: %v", test.expected, actual)
			}
		}
	}
}
