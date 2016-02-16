package pilosa

import (
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
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

// Profile represents vertical column in a database.
// A profile can have a set of attributes attached to it.
type Profile struct {
	ID    uint64                 `json:"id"`
	Attrs map[string]interface{} `json:"attrs,omitempty"`
}

// encodeProfiles converts a into its internal representation.
func encodeProfiles(a []*Profile) []*internal.Profile {
	other := make([]*internal.Profile, len(a))
	for i := range a {
		other[i] = encodeProfile(a[i])
	}
	return other
}

// decodeProfiles converts a from its internal representation.
func decodeProfiles(a []*internal.Profile) []*Profile {
	other := make([]*Profile, len(a))
	for i := range a {
		other[i] = decodeProfile(a[i])
	}
	return other
}

// encodeProfile converts p into its internal representation.
func encodeProfile(p *Profile) *internal.Profile {
	return &internal.Profile{
		ID:    proto.Uint64(p.ID),
		Attrs: encodeAttrs(p.Attrs),
	}
}

// decodeProfile converts b from its internal representation.
func decodeProfile(pb *internal.Profile) *Profile {
	p := &Profile{
		ID: pb.GetID(),
	}

	if len(pb.GetAttrs()) > 0 {
		p.Attrs = make(map[string]interface{}, len(pb.GetAttrs()))
		for _, attr := range pb.GetAttrs() {
			k, v := decodeAttr(attr)
			p.Attrs[k] = v
		}
	}

	return p
}
