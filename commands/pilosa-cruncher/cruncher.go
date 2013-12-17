package main

import (
	"pilosa/cruncher"
)

func main() {
	cruncher := cruncher.NewCruncher()
	cruncher.Run()
}
