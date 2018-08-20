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
	if err != pilosa.ErrFieldRequired {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrFieldRequired, err)
	}

	cm.Field = "f"
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

	cmd := test.MustRunCluster(t, 1)[0]
	cm.Host = cmd.API.Node().URI.HostPort()

	cm.Index = "i"
	cm.Field = "f"
	cm.CreateSchema = true
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run doesn't work: %s", err)
	}
}

// Ensure that the ImportValue path runs.
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

	cmd := test.MustRunCluster(t, 1)[0]
	cm.Host = cmd.API.Node().URI.HostPort()

	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader("")))
	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"type": "int", "min": 0, "max": 100}}`)))

	cm.Index = "i"
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}
}

// Ensure that import with keys runs.
func TestImportCommand_RunKeys(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import-key.csv")
	file.Write([]byte("foo1,bar2\nfoo3,bar4\nfoo5,bar6"))
	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}

	cmd := test.MustRunCluster(t, 1)[0]
	cm.Host = cmd.API.Node().URI.HostPort()

	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader(`{"options":{"keys": true}}`)))
	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"keys": true}}`)))

	cm.Index = "i"
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with keys doesn't work: %s", err)
	}
}

// Ensure that integer import with keys runs.
func TestImportCommand_RunValueKeys(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import-key.csv")
	file.Write([]byte("foo1,2\nfoo3,4\nfoo5,6"))
	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}

	cmd := test.MustRunCluster(t, 1)[0]
	cm.Host = cmd.API.Node().URI.HostPort()

	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader(`{"options":{"keys": true}}`)))
	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"type": "int", "min": 0, "max": 100}}`)))

	cm.Index = "i"
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with keys doesn't work: %s", err)
	}
}

func TestImportCommand_InvalidFile(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	cm.Host = cmd.API.Node().URI.HostPort()
	cm.Index = "i"
	cm.Field = "f"
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
	req.Header.Add("Accept", "application/json")
	if err != nil {
		panic(err)
	}
	return req
}

// declare stdin, stdout, stderr
func GetIO(buf bytes.Buffer) (io.Reader, io.Writer, io.Writer) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	stdout := bufio.NewWriter(&buf)
	stderr := bufio.NewWriter(&buf)
	return stdin, stdout, stderr
}

func TestImportCommand_BugOverwriteValue(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import-value.csv")
	file.Write([]byte("0,17\n"))
	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}

	cm.Host = cmd.API.Node().URI.HostPort()

	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader("")))
	http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"type": "int", "min": 0, "max":2147483648 }}`)))

	cm.Index = "i"
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}

	file.Close()
	file, err = ioutil.TempFile("", "import-value2.csv")
	if err != nil {
		t.Fatalf("Error creating tempfile: %s", err)
	}
	file.Write([]byte("0,16\n"))
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}

	file.Close()
	file, err = ioutil.TempFile("", "import-value3.csv")
	if err != nil {
		t.Fatalf("Error creating tempfile: %s", err)
	}
	file.Write([]byte("0,19\n"))
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}
}
