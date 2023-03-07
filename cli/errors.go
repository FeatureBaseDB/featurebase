package cli

import (
	"github.com/featurebasedb/featurebase/v3/errors"
)

const (
	ErrOrganizationRequired errors.Code = "OrganizationRequired"
)

func NewErrOrganizationRequired() error {
	return errors.New(
		ErrOrganizationRequired,
		"organization required",
	)
}
