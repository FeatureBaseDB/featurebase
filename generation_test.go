// Copyright 2020 Pilosa Corp.
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
//
// +build generationparanoia

package pilosa

import (
	"runtime"
	"testing"
	"unsafe"
)

func TestGenerationPanic(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "none")
	defer f.Clean(t)

	for i := 0; i < f.MaxOpN; i++ {
		_, _ = f.setBit(0, uint64(i*32))
	}
	// force snapshot so we get a mmapped row...
	_ = f.Snapshot()
	_ = f.row(0)
	var prevData []byte

	if f.gen.(*mmapGeneration).data == nil {
		t.Fatalf("generation code didn't create a mapping, apparently?")
	}
	prevData = f.gen.(*mmapGeneration).data
	f.mu.Lock()
	_ = defaultSnapshotQueue.Immediate(f)
	f.mu.Unlock()
	runtime.GC()
	for i := 0; i < (f.MaxOpN / 2); i++ {
		_, _ = f.setBit(0, uint64(i*32)+23)
	}
	f.mu.Lock()
	defaultSnapshotQueue.Await(f)
	f.mu.Unlock()
	runtime.GC()
	newData := f.gen.(*mmapGeneration).data
	if unsafe.Pointer(&prevData[0]) == unsafe.Pointer(&newData[0]) {
		t.Fatalf("test can't run usefully, didn't get new data pointer")
	}
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	err := f.gen.Transaction(wp, func() error {
		prevData[0] = 0x3c
		return nil
	})
	if err == nil {
		t.Fatalf("expected a panic to get caught, but nothing happened")
	}
	if err.Error() != "invalid memory access during transaction" {
		t.Fatalf("expected \"invalid memory access during transaction\", got %q", err.Error())
	}
}
