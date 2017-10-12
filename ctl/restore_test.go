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
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestRestoreCommand_FileRequired(t *testing.T) {
	cm := RestoreCommand{}
	ctx := context.Background()
	err := cm.Run(ctx)
	if err.Error() != "backup file required" {
		t.Fatalf("expect error: output file required, actual: '%s'", err)
	}
}

func TestRestoreCommand_Run(t *testing.T) {
	file, err := ioutil.TempFile("", "restore.csv")
	if err != nil {
		t.Fatal(err)
	}
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

	cm := NewRestoreCommand(stdin, stdout, stderr)
	cm.Path = file.Name()
	cm.Index = "i"
	cm.Frame = "f"
	cm.View = pilosa.ViewStandard
	cm.Host = s.Host()
	cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Backup Run doesn't work: %s", err)
	}
}

// declare stdin, stdout, stderr
func GetIO(buf bytes.Buffer) (io.Reader, io.Writer, io.Writer) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	stdout := bufio.NewWriter(&buf)
	stderr := bufio.NewWriter(&buf)
	return stdin, stdout, stderr
}
