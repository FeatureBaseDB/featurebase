package cmd_test

import (
	"strings"
	"testing"
)

func TestServerHelp(t *testing.T) {
	output := ExecNewRootCommand(t, "server", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") {
		t.Fatalf("Command 'server --help' not working, got: %s", output)
	}
}
