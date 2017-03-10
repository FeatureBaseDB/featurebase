package pilosa

import (
	"errors"
	"regexp"

	"github.com/pilosa/pilosa/internal"
)

// System errors.
var (
	ErrHostRequired = errors.New("host required")

	ErrDatabaseRequired = errors.New("database required")
	ErrDatabaseExists   = errors.New("database already exists")
	ErrDatabaseNotFound = errors.New("database not found")

	// ErrFrameRequired is returned when no frame is specified.
	ErrFrameRequired = errors.New("frame required")
	ErrFrameExists   = errors.New("frame already exists")
	ErrFrameNotFound = errors.New("frame not found")

	ErrName = errors.New("invalid database or frame's name, must match [a-z0-9_-]")

	// ErrFragmentNotFound is returned when a fragment does not exist.
	ErrFragmentNotFound = errors.New("fragment not found")
	ErrQueryRequired    = errors.New("query required")
)

// Regular expression to valuate db and frame's name
// Todo: remove . when frame doesn't require . for topN
var nameRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,64}$`)

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
		ID:    p.ID,
		Attrs: encodeAttrs(p.Attrs),
	}
}

// decodeProfile converts b from its internal representation.
func decodeProfile(pb *internal.Profile) *Profile {
	p := &Profile{
		ID: pb.ID,
	}

	if len(pb.Attrs) > 0 {
		p.Attrs = make(map[string]interface{}, len(pb.Attrs))
		for _, attr := range pb.Attrs {
			k, v := decodeAttr(attr)
			p.Attrs[k] = v
		}
	}

	return p
}

// TimeFormat is the go-style time format used to parse string dates.
const TimeFormat = "2006-01-02T15:04"

// Restrict name using regex
func ValidateName(name string) error {
	validName := nameRegexp.Match([]byte(name))
	if validName == false {
		return ErrName
	}
	return nil
}
