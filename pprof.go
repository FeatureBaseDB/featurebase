// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	_ "net/http/pprof" // Imported for its side-effect of registering pprof endpoints with the server.

	"github.com/featurebasedb/featurebase/v3/storage"
	"github.com/featurebasedb/featurebase/v3/vprint"
)

// CPUProfileForDur (where "Dur" is short for "Duration"), is used for
// performance tuning during development. It's only called—but is currently
// commented out—in holder.go.
func CPUProfileForDur(dur time.Duration, outpath string) {
	// per-query pprof output:
	backend := storage.DefaultBackend
	path := outpath + "." + backend
	f, err := os.Create(path)
	vprint.PanicOn(err)

	if dur == 0 {
		dur = time.Minute
	}
	vprint.AlwaysPrintf("starting cpu profile for dur '%v', output to '%v'", dur, path)
	_ = pprof.StartCPUProfile(f)
	go func() {
		<-time.After(dur)
		pprof.StopCPUProfile()
		f.Close()
		vprint.AlwaysPrintf("stopping cpu profile after dur '%v', output: '%v'", dur, path)
	}()
}

// MemProfileForDur (where "Dur" is short for "Duration"), is used for
// performance tuning during development. It's only called—but is currently
// commented out—in holder.go.
func MemProfileForDur(dur time.Duration, outpath string) {
	// per-query pprof output:
	backend := storage.DefaultBackend
	path := outpath + "." + backend
	f, err := os.Create(path)
	vprint.PanicOn(err)

	if dur == 0 {
		dur = time.Minute
	}
	vprint.AlwaysPrintf("will write memory profile after dur '%v', output to '%v'", dur, path)
	go func() {
		<-time.After(dur)
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			vprint.PanicOn(fmt.Sprintf("could not write memory profile: %v", err))
		}
		f.Close()
		vprint.AlwaysPrintf("wrote memory profile after dur '%v', output: '%v'", dur, path)
	}()
}

type pprofProfile struct {
	fdCpu *os.File
}

var _ = newPprof
var _ = pprofProfile{}

// for manually calling Close() to stop profiling.
func newPprof() (pp *pprofProfile) {
	pp = &pprofProfile{}
	f, err := os.Create("cpu.manual.pprof")
	vprint.PanicOn(err)
	pp.fdCpu = f

	_ = pprof.StartCPUProfile(pp.fdCpu)
	return
}

func (pp *pprofProfile) Close() {

	pprof.StopCPUProfile()
	pp.fdCpu.Close()

	f, err := os.Create("mem.manual.pprof")
	vprint.PanicOn(err)

	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		vprint.PanicOn(fmt.Sprintf("could not write memory profile: %v", err))
	}
	f.Close()
}
