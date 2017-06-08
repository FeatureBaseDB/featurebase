// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"errors"
	"regexp"

	"github.com/pilosa/pilosa/internal"
)

// System errors.
var (
	ErrHostRequired = errors.New("host required")

	ErrIndexRequired = errors.New("index required")
	ErrIndexExists   = errors.New("index already exists")
	ErrIndexNotFound = errors.New("index not found")

	// ErrFrameRequired is returned when no frame is specified.
	ErrFrameRequired        = errors.New("frame required")
	ErrFrameExists          = errors.New("frame already exists")
	ErrFrameNotFound        = errors.New("frame not found")
	ErrFrameInverseDisabled = errors.New("frame inverse disabled")
	ErrColumnRowLabelEqual  = errors.New("column and row labels cannot be equal")

	ErrInvalidView      = errors.New("invalid view")
	ErrInvalidCacheType = errors.New("invalid cache type")

	ErrName  = errors.New("invalid index or frame's name, must match [a-z0-9_-]")
	ErrLabel = errors.New("invalid row or column label, must match [A-Za-z0-9_-]")

	// ErrFragmentNotFound is returned when a fragment does not exist.
	ErrFragmentNotFound = errors.New("fragment not found")
	ErrQueryRequired    = errors.New("query required")
	ErrTooManyWrites    = errors.New("too many write commands")
)

// Regular expression to validate index and frame names.
var nameRegexp = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// Regular expression to validate row and column labels.
var labelRegexp = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)

// ColumnAttrSet represents a set of attributes for a vertical column in an index.
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

// ValidateName ensures that the name is a valid format.
func ValidateName(name string) error {
	if nameRegexp.Match([]byte(name)) == false {
		return ErrName
	}
	return nil
}

// ValidateLabel ensures that the label is a valid format.
func ValidateLabel(label string) error {
	if labelRegexp.Match([]byte(label)) == false {
		return ErrLabel
	}
	return nil
}
