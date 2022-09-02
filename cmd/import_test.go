// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd_test

import (
	"strings"
	"testing"

	pilosa "github.com/molecula/featurebase/v3"

	"github.com/molecula/featurebase/v3/cmd"
	"github.com/molecula/featurebase/v3/pql"
)

func TestImportHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "import", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "featurebase import") || err != nil {
		t.Fatalf("Command 'import --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestImportConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"import"},
			env:  map[string]string{"PILOSA_HOST": "localhost:12345"},
			cfgFileContent: `
index = "myindex"
field = "f1"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Importer.Host, "localhost:12345")
				v.Check(cmd.Importer.Index, "myindex")
				v.Check(cmd.Importer.Field, "f1")
				return v.Error()
			},
		},
		{
			args: []string{"import", "--index", "i1", "--field", "f1", "--field-keys", "--field-min", "-10", "--field-max", "100"},
			env:  map[string]string{},
			validation: func() error {
				v := validator{}
				v.Check(cmd.Importer.Index, "i1")
				v.Check(cmd.Importer.Field, "f1")
				v.Check(cmd.Importer.FieldOptions, pilosa.FieldOptions{
					Keys:      true,
					Max:       pql.NewDecimal(100, 0),
					Min:       pql.NewDecimal(-10, 0),
					CacheType: pilosa.CacheTypeRanked,
					CacheSize: 50000,
				})
				return v.Error()
			},
		},
		{
			args: []string{"import", "--index", "i1", "--field", "f1", "--field-time-quantum", "YMD"},
			env:  map[string]string{},
			validation: func() error {
				v := validator{}
				v.Check(cmd.Importer.Index, "i1")
				v.Check(cmd.Importer.Field, "f1")
				v.Check(cmd.Importer.FieldOptions, pilosa.FieldOptions{
					TimeQuantum: "YMD",
					CacheType:   pilosa.CacheTypeRanked,
					CacheSize:   50000,
				})
				return v.Error()
			},
		},
		{
			args: []string{"import", "--index", "i1", "--field", "f1", "--field-cache-type", "lru", "--field-cache-size", "100"},
			env:  map[string]string{},
			validation: func() error {
				v := validator{}
				v.Check(cmd.Importer.Index, "i1")
				v.Check(cmd.Importer.Field, "f1")
				v.Check(cmd.Importer.FieldOptions, pilosa.FieldOptions{
					CacheType: "lru",
					CacheSize: 100,
				})
				return v.Error()
			},
		},
		{
			args: []string{"import", "--index", "i1", "--field", "f1", "--clear", "true"},
			env:  map[string]string{},
			validation: func() error {
				v := validator{}
				v.Check(cmd.Importer.Index, "i1")
				v.Check(cmd.Importer.Field, "f1")
				v.Check(cmd.Importer.Clear, true)
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
