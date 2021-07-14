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

func TestCheckHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "check", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "featurebase check") || err != nil {
		t.Fatalf("Command 'check --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestCheckNoPath(t *testing.T) {
	output, err := ExecNewRootCommand(t, "check")
	if !strings.Contains(err.Error(), "path required") {
		t.Fatalf("Command 'check' without args should error but: err: '%v', output: '%v'", err, output)
	}
}
