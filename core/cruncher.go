package core

import (
    "github.com/davecgh/go-spew/spew"
)


type Cruncher struct {
}

func (cruncher *Cruncher) Run(port int) {

    spew.Dump("Cruncher.Run")
    spew.Dump(port)

}
