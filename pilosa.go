package pilosa

//go:generate protoc --gogo_out=. internal/internal.proto

// Version represents the current running version of Pilosa.
var Version string
