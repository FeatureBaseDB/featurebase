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

	"github.com/pilosa/pilosa/v2/cmd"
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
