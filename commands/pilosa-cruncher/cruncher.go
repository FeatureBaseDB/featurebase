package main

import (
	"flag"
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
}
