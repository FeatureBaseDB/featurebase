package main

import (
	_ "circuit/load/cmd"
	"circuit/use/circuit"
	"pilosa/cruncher"
	"log"
)

func main() {
	log.Println("Spawning...")

	retrn, addr, err := circuit.Spawn("localhost", []string{"/cruncher"}, cruncher.App{})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(retrn, addr)

	circuit.Hang()
}

