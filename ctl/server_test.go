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

package ctl

import (
	"bytes"
	"testing"

	"github.com/molecula/featurebase/v2/server"
	"github.com/spf13/cobra"
)

func TestBuildServerFlags(t *testing.T) {
	cm := &cobra.Command{}
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	Server := server.NewCommand(stdin, stdout, stderr)
	BuildServerFlags(cm, Server)
	if cm.Flags().Lookup("data-dir").Name == "" {
		t.Fatal("data-dir flag is required")
	}
	if cm.Flags().Lookup("log-path").Name == "" {
		t.Fatal("log-path flag is required")
	}
}
