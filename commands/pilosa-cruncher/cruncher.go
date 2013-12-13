package main

import (
	"pilosa/cruncher"
	"pilosa/config"
)

func main() {
	cruncher := cruncher.NewCruncher()
	cruncher.Run(config.GetInt("port_tcp"))
}
