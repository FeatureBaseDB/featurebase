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
	"runtime"
	"testing"
	"time"
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
