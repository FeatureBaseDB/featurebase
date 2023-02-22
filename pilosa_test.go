// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3"
	_ "github.com/featurebasedb/featurebase/v3/test"
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

func TestValidateName(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "a_name", err: nil},
		{name: "a-name", err: nil},
		{name: "a-name-10", err: nil},
		{name: "A-name", err: fmt.Errorf("'A-name': %s", pilosa.ErrName)},
		{name: "-a-name", err: fmt.Errorf("'-a-name': %s", pilosa.ErrName)},
		{name: "8th_name", err: fmt.Errorf("'8th_name': %s", pilosa.ErrName)},
		{name: "Θ_name", err: fmt.Errorf("'Θ_name': %s", pilosa.ErrName)},
		{name: "a_NaMe", err: fmt.Errorf("'a_NaMe': %s", pilosa.ErrName)},
		{name: "indexΘname", err: nil},
	}
	for _, test := range tests {
		err := pilosa.ValidateName(test.name)
		if !(err == nil && test.err == nil) {
			if err.Error() != test.err.Error() {
				t.Errorf("expected error: %v, but got: %v", test.err, err)
			}
		}
	}
}
