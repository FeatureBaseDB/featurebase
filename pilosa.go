// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"regexp"
	"time"

	"github.com/featurebasedb/featurebase/v3/disco"
	pnet "github.com/featurebasedb/featurebase/v3/net"
	"github.com/pkg/errors"
)

// System errors.
var (
	ErrHostRequired = errors.New("host required")

	ErrIndexRequired = errors.New("index required")
	ErrIndexExists   = disco.ErrIndexExists
	ErrIndexNotFound = errors.New("index not found")

	ErrInvalidAddress = errors.New("invalid address")
	ErrInvalidSchema  = errors.New("invalid schema")

	ErrForeignIndexNotFound = errors.New("foreign index not found")

	// ErrFieldRequired is returned when no field is specified.
	ErrFieldRequired  = errors.New("field required")
	ErrColumnRequired = errors.New("column required")
	ErrFieldExists    = disco.ErrFieldExists
	ErrFieldNotFound  = errors.New("field not found")

	ErrBSIGroupNotFound         = errors.New("bsigroup not found")
	ErrBSIGroupExists           = errors.New("bsigroup already exists")
	ErrBSIGroupNameRequired     = errors.New("bsigroup name required")
	ErrInvalidBSIGroupType      = errors.New("invalid bsigroup type")
	ErrInvalidBSIGroupRange     = errors.New("invalid bsigroup range")
	ErrInvalidBSIGroupValueType = errors.New("invalid bsigroup value type")
	ErrBSIGroupValueTooLow      = errors.New("value too low for configured field range")
	ErrBSIGroupValueTooHigh     = errors.New("value too high for configured field range")
	ErrInvalidRangeOperation    = errors.New("invalid range operation")
	ErrInvalidBetweenValue      = errors.New("invalid value for between operation")
	ErrDecimalOutOfRange        = errors.New("decimal value out of range")

	ErrViewRequired     = errors.New("view required")
	ErrViewExists       = disco.ErrViewExists
	ErrInvalidView      = errors.New("invalid view")
	ErrInvalidCacheType = errors.New("invalid cache type")

	ErrName = errors.New("invalid index or field name, must match [a-z][a-z0-9Θ_-]* and contain at most 300 characters")

	// ErrFragmentNotFound is returned when a fragment does not exist.
	ErrFragmentNotFound = errors.New("fragment not found")
	ErrQueryRequired    = errors.New("query required")
	ErrQueryCancelled   = errors.New("query cancelled")
	ErrQueryTimeout     = errors.New("query timeout")
	ErrTooManyWrites    = errors.New("too many write commands")

	// TODO(2.0) poorly named - used when a *node* doesn't own a shard. Probably
	// we won't need this error at all by 2.0 though.
	ErrClusterDoesNotOwnShard = errors.New("node does not own shard")

	// ErrPreconditionFailed is returned when specified index/field createdAt timestamps don't match
	ErrPreconditionFailed = errors.New("precondition failed")

	ErrNodeIDNotExists = errors.New("node with provided ID does not exist")
	ErrNodeNotPrimary  = errors.New("node is not the primary")

	ErrNotImplemented            = errors.New("not implemented")
	ErrFieldsArgumentRequired    = errors.New("fields argument required")
	ErrExpectedFieldListArgument = errors.New("expected field list argument")

	ErrIntFieldWithKeys       = errors.New("int field cannot be created with 'keys=true' option")
	ErrDecimalFieldWithKeys   = errors.New("decimal field cannot be created with 'keys=true' option")
	ErrTimestampFieldWithKeys = errors.New("timestamp field cannot be created with 'keys=true' option")
)

// apiMethodNotAllowedError wraps an error value indicating that a particular
// API method is not allowed in the current cluster state.
type apiMethodNotAllowedError struct {
	error
}

// newAPIMethodNotAllowedError returns err wrapped in an ApiMethodNotAllowedError.
func newAPIMethodNotAllowedError(err error) apiMethodNotAllowedError {
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

// Unwrap makes it so that a ConflictError wrapping ErrFieldExists gets a
// true from errors.Is(ErrFieldExists).
func (c ConflictError) Unwrap() error {
	return c.error
}

// NotFoundError wraps an error value to signify that a resource was not found
// such that in an HTTP scenario, http.StatusNotFound would be returned.
type NotFoundError error

// newNotFoundError returns err wrapped in a NotFoundError.
func newNotFoundError(err error, name string) NotFoundError {
	return NotFoundError(errors.WithMessage(err, name))
}

type PreconditionFailedError struct {
	error
}

// newPreconditionFailedError returns err wrapped in a PreconditionFailedError.
func newPreconditionFailedError(err error) PreconditionFailedError {
	return PreconditionFailedError{err}
}

// Regular expression to validate index and field names.
var nameRegexp = regexp.MustCompile(`^[a-z][a-z0-9Θ_-]{0,299}$`)

// TimeFormat is the go-style time format used to parse string dates.
const TimeFormat = "2006-01-02T15:04"

// ValidateName ensures that the index or field or view name is a valid format.
func ValidateName(name string) error {
	if !nameRegexp.Match([]byte(name)) {
		return errors.Wrapf(ErrName, "'%s'", name)
	}
	return nil
}

func timestamp() int64 {
	return time.Now().UnixNano()
}

// AddressWithDefaults converts addr into a valid address,
// using defaults when necessary.
func AddressWithDefaults(addr string) (*pnet.URI, error) {
	if addr == "" {
		return pnet.DefaultURI(), nil
	}
	return pnet.NewURIFromAddress(addr)
}
