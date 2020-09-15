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

package pilosa

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	_ "net/http/pprof" // Imported for its side-effect of registering pprof endpoints with the server.
)

func CPUProfileForDur(dur time.Duration, outpath string) {

	// per-query pprof output:
	txsrc := os.Getenv("PILOSA_TXSRC")
	if txsrc == "" {
		txsrc = "roaring"
	}
	path := outpath + "." + txsrc
	f, err := os.Create(path)
	panicOn(err)

	if dur == 0 {
		dur = time.Minute
	}
	AlwaysPrintf("starting cpu profile for dur '%v', output to '%v'", dur, path)
	_ = pprof.StartCPUProfile(f)
	go func() {
		<-time.After(dur)
		pprof.StopCPUProfile()
		f.Close()
		AlwaysPrintf("stopping cpu profile after dur '%v', output: '%v'", dur, path)
	}()
}

func MemProfileForDur(dur time.Duration, outpath string) {

	// per-query pprof output:
	txsrc := os.Getenv("PILOSA_TXSRC")
	if txsrc == "" {
		txsrc = "roaring"
	}
	path := outpath + "." + txsrc
	f, err := os.Create(path)
	panicOn(err)

	if dur == 0 {
		dur = time.Minute
	}
	AlwaysPrintf("will write memory profile after dur '%v', output to '%v'", dur, path)
	go func() {
		<-time.After(dur)
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			panic(fmt.Sprintf("could not write memory profile: %v", err))
		}
		f.Close()
		AlwaysPrintf("wrote memory profile after dur '%v', output: '%v'", dur, path)
	}()
}