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

package pilosa

import (
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/pilosa/pilosa/roaring"
)

// Ensure the file handle count is working
func TestCountOpenFiles(t *testing.T) {
	// Windows is not supported yet
	if runtime.GOOS == "windows" {
		t.Skip("Skipping unsupported countOpenFiles test on Windows.")
	}
	count, err := countOpenFiles()
	if err != nil {
		t.Errorf("countOpenFiles failed: %s", err)
	}
	if count == 0 {
		t.Error("countOpenFiles returned invalid value 0.")
	}
}

func TestMonitorAntiEntropyZero(t *testing.T) {

	td, err := ioutil.TempDir(*TempDir, "")
	if err != nil {
		t.Fatalf("getting temp dir: %v", err)
	}
	s, err := NewServer(OptServerDataDir(td),
		OptServerAntiEntropyInterval(0))
	if err != nil {
		t.Fatalf("making new server: %v", err)
	}

	ch := make(chan struct{})
	go func() {
		s.monitorAntiEntropy()
		close(ch)
	}()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("monitorAntiEntropy should have returned immediately with duration 0")
	}
}

func TestOnlyOpenOwnedFiles(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa")
	if err != nil {
		t.Fatalf("getting temp dir: %v", err)
	}
	defer func() {
		err := os.RemoveAll(path)
		if err != nil {
			t.Logf("cleaning up temp dir: %v", err)
		}
	}()
	bm := roaring.NewFileBitmap(1, 2, 3)
	err = os.MkdirAll(path+"/i/f/views/standard/fragments", os.ModeDir|os.ModePerm)
	if err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	one, err := os.Create(path + "/i/f/views/standard/fragments/1")
	if err != nil {
		t.Fatalf("creating one: %v", err)
	}
	two, err := os.Create(path + "/i/f/views/standard/fragments/2")
	if err != nil {
		t.Fatalf("creating two: %v", err)
	}
	_, err = bm.WriteTo(one)
	if err != nil {
		t.Fatalf("writing to one: %v", err)
	}
	_, err = bm.WriteTo(two)
	if err != nil {
		t.Fatalf("writing to two: %v", err)
	}

	h := NewHolder()
	h.Path = path
	h.shardValidatorFunc = func(index string, shard uint64) bool {
		return shard == 1
	}

	err = h.Open()
	if err != nil {
		t.Fatalf("opening holder: %v", err)
	}

	view := h.Index("i").Field("f").view("standard")

	if len(view.fragments) != 1 {
		t.Errorf("should have one fragment, but have: %d", len(view.fragments))
	}

	if _, ok := view.fragments[1]; !ok {
		t.Errorf("should have fragment 1, but fragments: %#v", view.fragments)
	}
}
