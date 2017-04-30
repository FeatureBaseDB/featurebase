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
)

func TestSortHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "sort", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa sort") || err != nil {
		t.Fatalf("Command 'sort --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestSortNoPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "sort")
	if !strings.Contains(err.Error(), "path required") {
		t.Fatalf("Command 'sort' without args should error but: err: '%v', output: '%v'", err, output)
	}
}

func TestSortMultiPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "sort", "one", "two")
	if !strings.Contains(err.Error(), "only one path") {
		t.Fatalf("Command 'sort' without args should error but: err: '%v', output: '%v'", err, output)
	}
}
