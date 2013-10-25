package db

type Message struct {
	Key string `json:key`
	Data interface{} `json:data`
	Destination Location
}

type Envelope struct {
	Message *Message
	Location *Location
}

