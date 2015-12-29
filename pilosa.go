package pilosa

import (
	"errors"
)

//go:generate protoc --gogo_out=. internal/internal.proto

var (
	// ErrHostRequired is returned when excuting a remote operation without a host.
	ErrHostRequired = errors.New("host required")

	// ErrDatabaseRequired is returned when no database is specified.
	ErrDatabaseRequired = errors.New("database required")

	// ErrFrameRequired is returned when no frame is specified.
	ErrFrameRequired = errors.New("frame required")
)

// Version represents the current running version of Pilosa.
var Version string
