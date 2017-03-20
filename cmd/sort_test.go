package cmd_test

import (
	"strings"
	"testing"
)

func TestSortHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "sort", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa sort") || err != nil {
		t.Fatalf("Command 'sort --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestSortNoPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "sort")
	if !strings.Contains(err.Error(), "path required") {
		t.Fatalf("Command 'sort' without args should error but: err: '%v', output: '%v'", err, output)
	}
}

func TestSortMultiPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "sort", "one", "two")
	if !strings.Contains(err.Error(), "only one path") {
		t.Fatalf("Command 'sort' without args should error but: err: '%v', output: '%v'", err, output)
	}
}
