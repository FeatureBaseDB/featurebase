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
	ErrFrameRequired        = errors.New("frame required")
	ErrFrameExists          = errors.New("frame already exists")
	ErrFrameNotFound        = errors.New("frame not found")
	ErrFrameInverseDisabled = errors.New("frame inverse disabled")

	ErrInvalidView      = errors.New("invalid view")
	ErrInvalidCacheType = errors.New("invalid cache type")

	ErrName = errors.New("invalid database or frame's name, must match [a-z0-9_-]")

	// ErrFragmentNotFound is returned when a fragment does not exist.
	ErrFragmentNotFound = errors.New("fragment not found")
	ErrQueryRequired    = errors.New("query required")
)

// Regular expression to valuate db and frame's name
// Todo: remove . when frame doesn't require . for topN
var nameRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,64}$`)

// ColumnAttrSet represents a set of attributes for a vertical column in a database.
// Can have a set of attributes attached to it.
type ColumnAttrSet struct {
	ID    uint64                 `json:"id"`
	Attrs map[string]interface{} `json:"attrs,omitempty"`
}

// encodeColumnAttrSets converts a into its internal representation.
func encodeColumnAttrSets(a []*ColumnAttrSet) []*internal.ColumnAttrSet {
	other := make([]*internal.ColumnAttrSet, len(a))
	for i := range a {
		other[i] = encodeColumnAttrSet(a[i])
	}
	return other
}

// decodeColumnAttrSets converts a from its internal representation.
func decodeColumnAttrSets(a []*internal.ColumnAttrSet) []*ColumnAttrSet {
	other := make([]*ColumnAttrSet, len(a))
	for i := range a {
		other[i] = decodeColumnAttrSet(a[i])
	}
	return other
}

// encodeColumnAttrSet converts set into its internal representation.
func encodeColumnAttrSet(set *ColumnAttrSet) *internal.ColumnAttrSet {
	return &internal.ColumnAttrSet{
		ID:    set.ID,
		Attrs: encodeAttrs(set.Attrs),
	}
}

// decodeColumnAttrSet converts b from its internal representation.
func decodeColumnAttrSet(pb *internal.ColumnAttrSet) *ColumnAttrSet {
	set := &ColumnAttrSet{
		ID: pb.ID,
	}

	if len(pb.Attrs) > 0 {
		set.Attrs = make(map[string]interface{}, len(pb.Attrs))
		for _, attr := range pb.Attrs {
			k, v := decodeAttr(attr)
			set.Attrs[k] = v
		}
	}

	return set
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
