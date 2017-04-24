package cmd_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/cmd"
)

func TestRestoreHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "restore", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa restore") || err != nil {
		t.Fatalf("Command 'restore --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestRestoreConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"restore", "--input-file", "/somefile"},
			env:  map[string]string{"PILOSA_HOST": "localhost:12345"},
			cfgFileContent: `
index = "myindex"
frame = "f1"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Restorer.Host, "localhost:12345")
				v.Check(cmd.Restorer.Index, "myindex")
				v.Check(cmd.Restorer.Frame, "f1")
				v.Check(cmd.Restorer.Path, "/somefile")
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
