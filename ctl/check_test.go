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
	"encoding/hex"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"context"
)

func TestCheckCommand_RunCacheFile(t *testing.T) {
	cacheFile := TempFileName("test", ".cache")

	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewCheckCommand(stdin, w, w)
	cm.Paths = []string{cacheFile}

	err := cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !strings.Contains(buf.String(), "ignoring cache file") {
		t.Fatalf("expect: ignoring cache file, actual: '%s'", err)
	}
}

func TestCheckCommand_RunSnapshot(t *testing.T) {
	snapshotFile := TempFileName("test", ".snapshotting")

	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewCheckCommand(stdin, w, w)
	cm.Paths = []string{snapshotFile}

	err := cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !strings.Contains(buf.String(), "ignoring snapshot file") {
		t.Fatalf("expect: ignoring snapshot file, actual: '%s'", err)
	}
}

func TestCheckCommand_Run(t *testing.T) {
	file, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("1234,1223"))
	file.Close()

	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewCheckCommand(stdin, w, w)
	cm.Paths = []string{file.Name()}

	err = cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !strings.HasPrefix(err.Error(), "checking bitmap: unmarshalling: reading roaring header:") {
		t.Fatalf("expect error: invalid roaring file, actual: '%s'", err)
	}
	//	Todo: need correct roaring file for happy path
}

// TempFileName generates a temporary filename with extension
func TempFileName(prefix, suffix string) string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return filepath.Join(os.TempDir(), prefix+hex.EncodeToString(randBytes)+suffix)
}
