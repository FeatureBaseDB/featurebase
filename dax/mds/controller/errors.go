package controller

import (
	"fmt"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/errors"
)

const (
	// ErrCodeCustom can be used to return a custom error message.
	ErrCodeCustom errors.Code = "CustomError"

	// ErrCodeInternal can be used when the cause of an error can't be
	// determined. It can be accompanied by a single string message.
	ErrCodeInternal errors.Code = "InternalError"

	// ErrCodeTODO can be used as a placeholder until a proper error code is
	// created and assigned.
	ErrCodeTODO errors.Code = "TODOError"

	ErrCodeNodeExists      errors.Code = "NodeExists"
	ErrCodeNodeKeyInvalid  errors.Code = "NodeKeyInvalid"
	ErrCodeNoAvailableNode errors.Code = "NoAvailableNode"

	ErrCodeRoleTypeInvalid errors.Code = "RoleTypeInvalid"

	ErrCodeDirectiveSendFailure errors.Code = "DirectiveSendFailure"

	ErrCodeInvalidRequest errors.Code = "InvalidRequest"

	ErrCodeUnassignedJobs errors.Code = "UnassignedJobs"

	UndefinedErrorMessage string = "undefined message format"
)

// NewErrCustom can be used to return a custom error message.
func NewErrCustom() error {
	return errors.New(
		ErrCodeCustom,
		"",
	)
}

// NewErrInternal can be used when the cause of an error can't be determined. It
// can be accompanied by a single string message.
func NewErrInternal(msg string) error {
	return errors.New(
		ErrCodeInternal,
		fmt.Sprintf("internal error: %s", msg),
	)
}

func NewErrNodeExists(addr dax.Address) error {
	return errors.New(
		ErrCodeNodeExists,
		fmt.Sprintf("node '%s' already exists", addr),
	)
}

func NewErrNodeKeyInvalid(addr dax.Address) error {
	return errors.New(
		ErrCodeNodeKeyInvalid,
		fmt.Sprintf("node key '%s' is invalid", addr),
	)
}

func NewErrNoAvailableNode() error {
	return errors.New(
		ErrCodeNoAvailableNode,
		"no available node",
	)
}

func NewErrRoleTypeInvalid(roleType dax.RoleType) error {
	return errors.New(
		ErrCodeRoleTypeInvalid,
		fmt.Sprintf("role type '%s' is invalid", roleType),
	)
}

func NewErrDirectiveSendFailure(msg string) error {
	return errors.New(
		ErrCodeDirectiveSendFailure,
		fmt.Sprintf("directive failed to send: %s", msg),
	)
}

func NewErrInvalidRequest(msg string) error {
	return errors.New(
		ErrCodeInvalidRequest,
		fmt.Sprintf("invalid request: %s", msg),
	)
}

func NewErrUnassignedJobs(jobs []dax.Job) error {
	return errors.New(
		ErrCodeUnassignedJobs,
		fmt.Sprintf("found %d unassigned jobs", len(jobs)),
	)
}
