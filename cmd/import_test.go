// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa"

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
					Max:       100,
					Min:       -10,
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
	}
	executeDry(t, tests)
}
