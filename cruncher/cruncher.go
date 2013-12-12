package cruncher

import (
    "github.com/davecgh/go-spew/spew"
    "pilosa/index"
)


type Cruncher struct {
    close_chan chan bool
}

func (cruncher *Cruncher) Run(port int) {
    spew.Dump("Cruncher.Run")
    spew.Dump(port)
     web_api:= index.NewFragmentContainer()
//        web_api.AddFragment("general", "25", 0, "AAA-BBB-CCC") 
//        web_api.AddFragment("general", "25", 1, "AAA-BBB-CCC") 
//        web_api.AddFragment("general", "25", 2, "AAA-BBB-CCC") 

started:= make(chan bool)
go web_api.RunServer(port , cruncher.close_chan ,started ) 
<-started
//server is listening and going

}
