package db

import (
        "testing"
        "pilosa/index"
        "log"
    )

func TestShard(t *testing.T) {
        f := NewShard(1638400,index.NewMemoryStorage())
        go f.Run()
        log.Println("REQUEST")
        f.              inbound <- Request{IdCount{7812}}
        f.inbound <- Request{UnionCount{7812    , 227149}}
        f.inbound <- Request{IntersectCount{7812, 227149}}
        f.inbound <- Request{Quit{}}
        <-f.done

}

