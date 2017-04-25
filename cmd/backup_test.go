package cmd_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/cmd"
)

func TestBackupHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "backup", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa backup") || err != nil {
		t.Fatalf("Command 'backup --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestBackupConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"backup", "--output-file", "/somefile"},
			env:  map[string]string{"PILOSA_HOST": "localhost:12345"},
			cfgFileContent: `
index = "myindex"
frame = "f1"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Backuper.Host, "localhost:12345")
				v.Check(cmd.Backuper.Index, "myindex")
				v.Check(cmd.Backuper.Frame, "f1")
				v.Check(cmd.Backuper.Path, "/somefile")
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
