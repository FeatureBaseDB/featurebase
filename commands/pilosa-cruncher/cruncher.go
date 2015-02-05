package main

import (
	"flag"
	log "github.com/cihub/seelog"
	"github.com/mitchellh/panicwrap"
	"os"
	"pilosa/core"
	"pilosa/cruncher"
	"runtime/pprof"
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	Build      string
)

func main() {
	exitStatus, err := panicwrap.BasicWrap(panicHandler)
	if err != nil {
		// Something went wrong setting up the panic wrapper. Unlikely,
		// but possible.
		panic(err)
	}

	if exitStatus >= 0 {
		os.Exit(exitStatus)
	}
	core.Build = Build

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Warn(err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	cruncher := cruncher.NewCruncher()
	cruncher.Run()
	log.Warn("STOP")
}

func panicHandler(output string) {
	// output contains the full output (including stack traces) of the
	// panic. Put it in a file or something.
	log.Warn("The child panicked:\n\n", output)
	os.Exit(1)
}
