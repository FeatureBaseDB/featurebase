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
	"context"
	"io/ioutil"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestBackupCommand_FileRequired(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	cm := NewBackupCommand(stdin, stdout, stderr)
	err := cm.Run(context.Background())
	if err.Error() != "output file required" {
		t.Fatalf("expect error: output file required, actual: %s", err)
	}

}

func TestBackupCommand_Run(t *testing.T) {

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	defer s.Close()
	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}
	s.Handler.URI = uri
	s.Handler.Cluster = test.NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder
	cm := NewBackupCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import.csv")

	cm.Index = "i"
	cm.Host = s.Host()
	cm.Frame = "f"
	cm.View = pilosa.ViewStandard
	cm.Path = file.Name()
	err = cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Command not working, error: '%s'", err)
	}
}
