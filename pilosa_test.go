package pilosa_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa"
	_ "github.com/pilosa/pilosa/test"
)

func TestValidateName(t *testing.T) {
	names := []string{
		"a", "ab", "ab1", "b-c", "d_e",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, name := range names {
		if pilosa.ValidateName(name) != nil {
			t.Fatalf("Should be valid index name: %s", name)
		}
	}
}

func TestValidateNameInvalid(t *testing.T) {
	names := []string{
		"", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "1", "_", "-",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
	}
	for _, name := range names {
		if pilosa.ValidateName(name) == nil {
			t.Fatalf("Should be invalid index name: %s", name)
		}
	}
}

func TestValidateLabel(t *testing.T) {
	labels := []string{
		"a", "ab", "ab1", "d_e", "A", "Bc", "B1", "aB", "b-c",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, label := range labels {
		if pilosa.ValidateLabel(label) != nil {
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
		if pilosa.ValidateLabel(label) == nil {
			t.Fatalf("Should be invalid label: %s", label)
		}
	}
}

func TestStringInSlice(t *testing.T) {
	list := []string{"localhost:10101", "localhost:10102", "localhost:10103"}
	substr := "localhost:10101"
	if !pilosa.StringInSlice(substr, list) {
		t.Fatalf("Expected substring %s in %v", substr, list)
	}
	substr = "10101"
	if pilosa.StringInSlice(substr, list) {
		t.Fatalf("Expected substring %s not in %v", substr, list)
	}
}

func TestContainsSubstring(t *testing.T) {
	list := []string{"localhost:10101", "localhost:10102", "localhost:10103"}
	substr := "10101"
	if !pilosa.ContainsSubstring(substr, list) {
		t.Fatalf("Expected substring %s contained in %v", substr, list)
	}
	substr = "4000"
	if pilosa.ContainsSubstring(substr, list) {
		t.Fatalf("Expected substring %s in not contained in %v", substr, list)
	}
}

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
		{addr: "[invalid][addr]:port", err: "invalid address"},
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
