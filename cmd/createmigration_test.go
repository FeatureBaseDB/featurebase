package cmd_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/cmd"
)

func TestCreateMigrationHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "create-migration", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa create-migration") || err != nil {
		t.Fatalf("Command 'create-migration --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestCreateMigrationConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"create-migration", "--host", "127.0.0.1:15000", "--src-config", "a.toml", "--dest-config", "b.toml", "--output-file", "somefile.json"},
			validation: func() error {
				v := validator{}
				v.Check(cmd.MigrationPlanner.Host, "127.0.0.1:15000")
				v.Check(cmd.MigrationPlanner.SrcConfig, "a.toml")
				v.Check(cmd.MigrationPlanner.DestConfig, "b.toml")
				v.Check(cmd.MigrationPlanner.OutputFileName, "/somefile.json")
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
