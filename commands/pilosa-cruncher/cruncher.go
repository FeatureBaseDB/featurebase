package main

import (
	"pilosa/cruncher"
	"pilosa/core"
	"flag"
	"log"
)

var locationString string

func init() {
	flag.StringVar(&locationString, "l", "127.0.0.1:1300", "ip:port to listen on")
	flag.Parse()
}

func main() {
	location, err := core.NewLocation(locationString)
	if err != nil {
		log.Fatal("Location not valid:", locationString)
	}

	cruncher := cruncher.NewCruncher(location)
	cruncher.Run()
}
