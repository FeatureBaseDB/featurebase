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
	"os"
	"strings"
	"testing"
)

func TestInspectCommand_Run(t *testing.T) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()

	cm := NewInspectCommand(stdin, w, w)
	file, err := ioutil.TempFile("", "inspectTest")
	file.Write([]byte("12358267538963"))
	file.Close()
	cm.Path = file.Name()
	err = cm.Run(context.Background())

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	if !strings.Contains(buf.String(), "unmarshaling bitmap...") {
		t.Fatalf("Inspect doesn't work: %s", err)
	}

	//	Todo: need correct roaring file for happy path
}
