package pilosa_test

import (
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
