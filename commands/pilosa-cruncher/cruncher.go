package main

import (
	"flag"
	"github.com/mitchellh/panicwrap"
	"log"
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
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	cruncher := cruncher.NewCruncher()
	cruncher.Run()
	log.Println("STOP")
}

func panicHandler(output string) {
	// output contains the full output (including stack traces) of the
	// panic. Put it in a file or something.
	log.Printf("The child panicked:\n\n%s\n", output)
	os.Exit(1)
}
