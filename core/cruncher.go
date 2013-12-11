package core

import (
    "github.com/davecgh/go-spew/spew"
)


type Cruncher struct {
}

func (cruncher *Cruncher) Run() {
    spew.Dump("Cruncher.Run")
}
