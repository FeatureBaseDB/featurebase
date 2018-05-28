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
	"net"
	"regexp"
	"strings"

	"github.com/pilosa/pilosa/internal"
)

// System errors.
var (
	ErrHostRequired = errors.New("host required")

	ErrIndexRequired = errors.New("index required")
	ErrIndexExists   = errors.New("index already exists")
	ErrIndexNotFound = errors.New("index not found")

	// ErrFrameRequired is returned when no frame is specified.
	ErrFrameRequired = errors.New("frame required")
	ErrFrameExists   = errors.New("frame already exists")
	ErrFrameNotFound = errors.New("frame not found")

	ErrFieldNotFound         = errors.New("field not found")
	ErrFieldExists           = errors.New("field already exists")
	ErrFieldNameRequired     = errors.New("field name required")
	ErrInvalidFieldType      = errors.New("invalid field type")
	ErrInvalidFieldRange     = errors.New("invalid field range")
	ErrInvalidFieldValueType = errors.New("invalid field value type")
	ErrFieldValueTooLow      = errors.New("field value too low")
	ErrFieldValueTooHigh     = errors.New("field value too high")
	ErrInvalidRangeOperation = errors.New("invalid range operation")
	ErrInvalidBetweenValue   = errors.New("invalid value for between operation")

	ErrInvalidView      = errors.New("invalid view")
	ErrInvalidCacheType = errors.New("invalid cache type")

	ErrName  = errors.New("invalid index or frame's name, must match [a-z0-9_-]")
	ErrLabel = errors.New("invalid row or column label, must match [A-Za-z0-9_-]")

	// ErrFragmentNotFound is returned when a fragment does not exist.
	ErrFragmentNotFound = errors.New("fragment not found")
	ErrQueryRequired    = errors.New("query required")
	ErrTooManyWrites    = errors.New("too many write commands")

	ErrClusterDoesNotOwnSlice = errors.New("cluster does not own slice")

	ErrNodeIDNotExists    = errors.New("node with provided ID does not exist")
	ErrNodeNotCoordinator = errors.New("node is not the coordinator")
	ErrResizeNotRunning   = errors.New("no resize job currently running")
)

// ApiMethodNotAllowedError wraps an error value indicating that a particular
// API method is not allowed in the current cluster state.
type ApiMethodNotAllowedError struct {
	error
}

// BadRequestError wraps an error value to signify that a request could not be
// read, decoded, or parsed such that in an HTTP scenario, http.StatusBadRequest
// would be returned.
type BadRequestError struct {
	error
}

// Regular expression to validate index and frame names.
var nameRegexp = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

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

// encodeColumnAttrSet converts set into its internal representation.
func encodeColumnAttrSet(set *ColumnAttrSet) *internal.ColumnAttrSet {
	return &internal.ColumnAttrSet{
		ID:    set.ID,
		Attrs: encodeAttrs(set.Attrs),
	}
}

// TimeFormat is the go-style time format used to parse string dates.
const TimeFormat = "2006-01-02T15:04"

// ValidateName ensures that the name is a valid format.
func ValidateName(name string) error {
	if !nameRegexp.Match([]byte(name)) {
		return ErrName
	}
	return nil
}

// StringInSlice checks for substring a in the slice.
func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// StringSlicesAreEqual determines if two string slices are equal.
func StringSlicesAreEqual(a, b []string) bool {

	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// SliceDiff returns the difference between two uint64 slices.
func SliceDiff(a, b []uint64) []uint64 {
	m := make(map[uint64]uint64)

	for _, y := range b {
		m[y]++
	}

	var ret []uint64
	for _, x := range a {
		if m[x] > 0 {
			m[x]--
			continue
		}
		ret = append(ret, x)
	}

	return ret
}

// ContainsSubstring checks to see if substring a is contained in any string in the slice.
func ContainsSubstring(a string, list []string) bool {
	for _, b := range list {
		if strings.Contains(b, a) {
			return true
		}
	}
	return false
}

// HostToIP converts host to an IP4 address based on net.LookupIP().
func HostToIP(host string) string {
	// if host is not an IP addr, check net.LookupIP()
	if net.ParseIP(host) == nil {
		hosts, err := net.LookupIP(host)
		if err != nil {
			return host
		}
		for _, h := range hosts {
			// this restricts pilosa to IP4
			if h.To4() != nil {
				return h.String()
			}
		}
	}
	return host
}

// AddressWithDefaults converts addr into a valid address,
// using defaults when necessary.
func AddressWithDefaults(addr string) (*URI, error) {
	if addr == "" {
		return DefaultURI(), nil
	} else {
		return NewURIFromAddress(addr)
	}
}
