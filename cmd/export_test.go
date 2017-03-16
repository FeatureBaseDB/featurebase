package cmd_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/cmd"
)

func TestExportHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "export", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa export") || err != nil {
		t.Fatalf("Command 'export --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestExportConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"export", "--output-file", "/somefile"},
			env:  map[string]string{"PILOSA_HOST": "localhost:12345"},
			cfgFileContent: `
database = "mydb"
frame = "f1"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Exporter.Host, "localhost:12345")
				v.Check(cmd.Exporter.Database, "mydb")
				v.Check(cmd.Exporter.Frame, "f1")
				v.Check(cmd.Exporter.Path, "/somefile")
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
