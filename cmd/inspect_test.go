package cmd_test

import (
	"strings"
	"testing"
)

func TestInspectHelp(t *testing.T) {
	output := ExecNewRootCommand(t, "inspect", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "pilosa inspect") {
		t.Fatalf("Command 'inspect --help' not working, got: %s", output)
	}
}
