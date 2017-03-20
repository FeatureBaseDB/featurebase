package cmd_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/cmd"
)

func TestImportHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "import", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa import") || err != nil {
		t.Fatalf("Command 'import --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestImportConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"import"},
			env:  map[string]string{"PILOSA_HOST": "localhost:12345"},
			cfgFileContent: `
database = "mydb"
frame = "f1"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Importer.Host, "localhost:12345")
				v.Check(cmd.Importer.Database, "mydb")
				v.Check(cmd.Importer.Frame, "f1")
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
