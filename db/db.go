package db

type Message struct {
	Data interface{} `json:data`
}

/*
import "pilosa/core"

type Message interface {
	Handle(*core.Service)
}


type Message struct {
	Key         string      `json:key`
	Data        interface{} `json:data`
	Destination Location
}
*/
