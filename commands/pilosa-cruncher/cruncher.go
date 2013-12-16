package main

import (
	"pilosa/config"
	"pilosa/cruncher"
)

func main() {
	cruncher := cruncher.NewCruncher()
	cruncher.Run(config.GetInt("port_tcp"))
}
