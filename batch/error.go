package batch

import "github.com/pkg/errors"

// Predefined batch-related errors.
var (
	ErrPreconditionFailed = errors.New("Precondition failed")
)
