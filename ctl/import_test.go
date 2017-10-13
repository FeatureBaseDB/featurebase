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
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestImportCommand_Validation(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	err := cm.Run(context.Background())
	if err != pilosa.ErrIndexRequired {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrIndexRequired, err)
	}

	cm.Index = "i"
	err = cm.Run(context.Background())
	if err != pilosa.ErrFrameRequired {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrFrameRequired, err)
	}

	cm.Frame = "f"
	err = cm.Run(context.Background())
	if err.Error() != "path required" {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", "path required", err)
	}
}

func TestImportCommand_Run(t *testing.T) {

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import.csv")
	file.Write([]byte("1,2\n3,4\n5,6"))
	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}

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
	cm.Host = s.Host()

	cm.Index = "i"
	cm.Frame = "f"
	cm.CreateSchema = true
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run doesn't work: %s", err)
	}
}

// Ensure that the ImportValue path runs (note: we have specifed a value
// for cm.Field. Because the handler doesn't return errors (it sends them
// to the logger), we don't get an error returned at `cm.Run()` even though
// we haven't setup frame `f` to be RangeEnabled.
func TestImportCommand_RunValue(t *testing.T) {

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import-value.csv")
	file.Write([]byte("1,2\n3,4\n5,6"))
	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}

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
	cm.Host = s.Host()

	http.DefaultClient.Do(MustNewHTTPRequest("POST", s.URL+"/index/i", strings.NewReader("")))
	http.DefaultClient.Do(MustNewHTTPRequest("POST", s.URL+"/index/i/frame/f", strings.NewReader("")))

	cm.Index = "i"
	cm.Frame = "f"
	cm.Field = "foo"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}
}

func TestImportCommand_InvalidFile(t *testing.T) {

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	cm.Host = pilosa.DefaultHost
	cm.Index = "i"
	cm.Frame = "f"
	file, err := ioutil.TempFile("", "import.csv")
	file.Write([]byte("a,2\n3,5\n5,6"))
	if err != nil {
		t.Fatal(err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid row id on row") {
		t.Fatalf("expect error: invalid row id on row, actual: %s", err)
	}

	file, err = ioutil.TempFile("", "import1.csv")
	file.Write([]byte("1,\n3,\n5,6"))
	if err != nil {
		t.Fatal(err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid column id on row") {
		t.Fatalf("expect error: invalid column id on row, actual: %s", err)
	}

	file, err = ioutil.TempFile("", "import1.csv")
	file.Write([]byte("1,2,34343\n1,3,54565,\n5,6,565"))
	if err != nil {
		t.Fatal(err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid timestamp on row") {
		t.Fatalf("expect error: invalid timestamp on row, actual: %s", err)
	}

	file, err = ioutil.TempFile("", "import1.csv")
	file.Write([]byte("1\n3\n5"))
	if err != nil {
		t.Fatal(err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "bad column count on row") {
		t.Fatalf("expect error: bad column count on row, actual: %s", err)
	}

}

// MustNewHTTPRequest creates a new HTTP request. Panic on error.
func MustNewHTTPRequest(method, urlStr string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err)
	}
	return req
}
