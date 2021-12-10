package cmd_test

import (
	"strings"
	"testing"
)

func TestCheckHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "check", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "featurebase check") || err != nil {
		t.Fatalf("Command 'check --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestCheckNoPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "check")
	if !strings.Contains(err.Error(), "path required") {
		t.Fatalf("Command 'check' without args should error but: err: '%v', output: '%v'", err, output)
	}
}
