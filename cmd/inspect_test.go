package cmd_test

import (
	"strings"
	"testing"
)

func TestInspectHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "inspect", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "featurebase inspect") || err != nil {
		t.Fatalf("Command 'inspect --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestInspectNoPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "inspect")
	if !strings.Contains(err.Error(), "path required") {
		t.Fatalf("Command 'inspect' without args should error but: err: '%v', output: '%v'", err, output)
	}
}

func TestInspectMultiPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "inspect", "one", "two")
	if !strings.Contains(err.Error(), "only one path") {
		t.Fatalf("Command 'inspect' without args should error but: err: '%v', output: '%v'", err, output)
	}
}
