package db

type Message struct {
	Key         string      `json:key`
	Data        interface{} `json:data`
	Destination Location
}
