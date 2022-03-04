// Copyright 2021 Molecula Corp. All rights reserved.
package cmd_test

import (
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/cmd"
)

func TestExportHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "export", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "featurebase export") || err != nil {
		t.Fatalf("Command 'export --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestExportConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"export", "--output-file", "/somefile"},
			env:  map[string]string{"PILOSA_HOST": "localhost:12345"},
			cfgFileContent: `
index = "myindex"
field = "f1"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Exporter.Host, "localhost:12345")
				v.Check(cmd.Exporter.Index, "myindex")
				v.Check(cmd.Exporter.Field, "f1")
				v.Check(cmd.Exporter.Path, "/somefile")
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
