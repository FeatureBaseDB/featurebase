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
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestSortCommand_Run(t *testing.T) {
	file, _ := ioutil.TempFile("", "file.csv")
	content := "3,3\n1,2\n2,4"
	file.Write([]byte(content))
	file.Close()

	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()

	cm := NewSortCommand(stdin, w, w)
	cm.Path = file.Name()
	err := cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	if err != nil {
		t.Fatal(err)
	} else if !strings.Contains(buf.String(), "1,2\n2,4\n3,3") {
		t.Fatalf("File is not sorted, actual result: %s", buf.String())
	}
}

func TestSortCommand_InvalidFile(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	file, _ := ioutil.TempFile("", "file.csv")
	file.Write([]byte("3,3\na,8\n2,4"))
	file.Close()

	cm := NewSortCommand(stdin, stdout, stderr)
	cm.Path = file.Name()
	err := cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid row id") {
		t.Fatalf("expect err: invalid row id, actual: %s", err)

	}

	file, _ = ioutil.TempFile("", "file.csv")
	file.Write([]byte("3,3\n1,a\n2,4"))
	file.Close()
	cm.Path = file.Name()
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid column id") {
		t.Fatalf("expect err: invalid column id, actual: %s", err)

	}

	file, _ = ioutil.TempFile("", "file.csv")
	file.Write([]byte("3,3,1234\n1,2,34345\n2,4"))
	file.Close()
	cm.Path = file.Name()
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid timestamp") {
		t.Fatalf("expect err: invalid timestamp, actual: %s", err)
	}
}
