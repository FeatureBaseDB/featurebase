package pilosa

import (
	"errors"
	"sort"

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

func encodeAttrs(m map[string]interface{}) []*internal.Attr {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	a := make([]*internal.Attr, len(keys))
	for i := range keys {
		a[i] = encodeAttr(keys[i], m[keys[i]])
	}
	return a
}

func decodeAttrs(pb []*internal.Attr) map[string]interface{} {
	m := make(map[string]interface{}, len(pb))
	for i := range pb {
		key, value := decodeAttr(pb[i])
		m[key] = value
	}
	return m
}

// encodeAttr converts a key/value pair into an Attr internal representation.
func encodeAttr(key string, value interface{}) *internal.Attr {
	pb := &internal.Attr{Key: proto.String(key)}
	switch value := value.(type) {
	case string:
		pb.StringValue = proto.String(value)
	case int64:
		pb.IntValue = proto.Int64(value)
	case bool:
		pb.BoolValue = proto.Bool(value)
	}
	return pb
}

// decodeAttr converts from an Attr internal representation to a key/value pair.
func decodeAttr(attr *internal.Attr) (key string, value interface{}) {
	if attr.StringValue != nil {
		return attr.GetKey(), attr.GetStringValue()
	} else if attr.IntValue != nil {
		return attr.GetKey(), attr.GetIntValue()
	} else if attr.BoolValue != nil {
		return attr.GetKey(), attr.GetBoolValue()
	}
	return attr.GetKey(), nil
}
