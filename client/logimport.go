package client

import (
	"encoding/gob"
	"io"
)

type importLog struct {
	Index     string
	Path      string
	Shard     uint64
	IsRoaring bool
	Timestamp int64 // Unix Nanoseconds
	Data      []byte
}

type encoder interface {
	Encode(thing interface{}) error
}

func newImportLogEncoder(w io.Writer) encoder {
	return gob.NewEncoder(w)
}

type decoder interface {
	Decode(thing interface{}) error
}

func newImportLogDecoder(r io.Reader) decoder {
	return gob.NewDecoder(r)
}
