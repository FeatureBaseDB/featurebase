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
)

// System errors.
var (
	ErrHostRequired = errors.New("host required")

	ErrIndexRequired = errors.New("index required")
	ErrIndexExists   = errors.New("index already exists")
	ErrIndexNotFound = errors.New("index not found")

	// ErrFieldRequired is returned when no field is specified.
	ErrFieldRequired = errors.New("field required")
	ErrFieldExists   = errors.New("field already exists")
	ErrFieldNotFound = errors.New("field not found")

	ErrBSIGroupNotFound         = errors.New("bsigroup not found")
	ErrBSIGroupExists           = errors.New("bsigroup already exists")
	ErrBSIGroupNameRequired     = errors.New("bsigroup name required")
	ErrInvalidBSIGroupType      = errors.New("invalid bsigroup type")
	ErrInvalidBSIGroupRange     = errors.New("invalid bsigroup range")
	ErrInvalidBSIGroupValueType = errors.New("invalid bsigroup value type")
	ErrBSIGroupValueTooLow      = errors.New("bsigroup value too low")
	ErrBSIGroupValueTooHigh     = errors.New("bsigroup value too high")
	ErrInvalidRangeOperation    = errors.New("invalid range operation")
	ErrInvalidBetweenValue      = errors.New("invalid value for between operation")

	ErrInvalidView      = errors.New("invalid view")
	ErrInvalidCacheType = errors.New("invalid cache type")

	ErrName  = errors.New("invalid index or field name, must match [a-z0-9_-]")
	ErrLabel = errors.New("invalid row or column label, must match [A-Za-z0-9_-]")

	ErrReservedName = errors.New("reserved index or field name")

	// ErrFragmentNotFound is returned when a fragment does not exist.
	ErrFragmentNotFound = errors.New("fragment not found")
	ErrQueryRequired    = errors.New("query required")
	ErrTooManyWrites    = errors.New("too many write commands")

	ErrClusterDoesNotOwnShard = errors.New("cluster does not own shard")

	ErrNodeIDNotExists    = errors.New("node with provided ID does not exist")
	ErrNodeNotCoordinator = errors.New("node is not the coordinator")
	ErrResizeNotRunning   = errors.New("no resize job currently running")

	ErrNotImplemented = errors.New("not implemented")
)

// apiMethodNotAllowedError wraps an error value indicating that a particular
// API method is not allowed in the current cluster state.
type apiMethodNotAllowedError struct {
	error
}

// newApiMethodNotAllowedError returns err wrapped in an ApiMethodNotAllowedError.
func newApiMethodNotAllowedError(err error) apiMethodNotAllowedError {
	return apiMethodNotAllowedError{err}
}

// BadRequestError wraps an error value to signify that a request could not be
// read, decoded, or parsed such that in an HTTP scenario, http.StatusBadRequest
// would be returned.
type BadRequestError struct {
	error
}

// NewBadRequestError returns err wrapped in a BadRequestError.
func NewBadRequestError(err error) BadRequestError {
	return BadRequestError{err}
}

// ConflictError wraps an error value to signify that a conflict with an
// existing resource occurred such that in an HTTP scenario, http.StatusConflict
// would be returned.
type ConflictError struct {
	error
}

// newConflictError returns err wrapped in a ConflictError.
func newConflictError(err error) ConflictError {
	return ConflictError{err}
}

// NotFoundError wraps an error value to signify that a resource was not found
// such that in an HTTP scenario, http.StatusNotFound would be returned.
type NotFoundError struct {
	error
}

// newNotFoundError returns err wrapped in a NotFoundError.
func newNotFoundError(err error) NotFoundError {
	return NotFoundError{err}
}

// Regular expression to validate index and field names.
var nameRegexp = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// ColumnAttrSet represents a set of attributes for a vertical column in an index.
// Can have a set of attributes attached to it.
type ColumnAttrSet struct {
	ID    uint64                 `json:"id"`
	Key   string                 `json:"key,omitempty"`
	Attrs map[string]interface{} `json:"attrs,omitempty"`
}

// TimeFormat is the go-style time format used to parse string dates.
const TimeFormat = "2006-01-02T15:04"

// validateName ensures that the name is a valid format.
func validateName(name string) error {
	if name == existenceFieldName {
		return ErrReservedName
	}
	if !nameRegexp.Match([]byte(name)) {
		return ErrName
	}
	return nil
}

// stringSlicesAreEqual determines if two string slices are equal.
func stringSlicesAreEqual(a, b []string) bool {

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

// AddressWithDefaults converts addr into a valid address,
// using defaults when necessary.
func AddressWithDefaults(addr string) (*URI, error) {
	if addr == "" {
		return defaultURI(), nil
	} else {
		return NewURIFromAddress(addr)
	}
}
