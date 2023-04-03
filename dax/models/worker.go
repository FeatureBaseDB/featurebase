package models

import (
	"encoding/json"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/gobuffalo/nulls"
	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
	"github.com/gobuffalo/validate/v3/validators"
	"github.com/gofrs/uuid"
)

// Worker is a node plus a role that gets assigned to a database and
// can be assigned jobs for that database.
type Worker struct {
	ID            uuid.UUID           `json:"id" db:"id"`
	Address       dax.Address         `json:"address" db:"address"`
	ServiceID     dax.WorkerServiceID `json:"service_id" db:"service_id"`
	Jobs          Jobs                `json:"jobs" has_many:"jobs" order_by:"name asc"`
	RoleCompute   bool                `json:"role_compute" db:"role_compute"`
	RoleTranslate bool                `json:"role_translate" db:"role_translate"`
	RoleQuery     bool                `json:"role_query" db:"role_query"`
	DatabaseID    nulls.String        `json:"database_id" db:"database_id"` // this can be empty which means the worker is unassigned. Probably get rid of this now that every worker is associated w/ a Service and every Service has an ID
	CreatedAt     time.Time           `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (t *Worker) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}

// SetRole applies a dax.RoleType to one of the boolean fields on the Worker
// model. It returns an error if the model does not support that role type.
func (t *Worker) SetRole(role dax.RoleType) error {
	switch role {
	case dax.RoleTypeCompute:
		t.RoleCompute = true
	case dax.RoleTypeTranslate:
		t.RoleTranslate = true
	case dax.RoleTypeQuery:
		t.RoleQuery = true
	default:
		errors.Errorf("invalid role type for worker: %s", role)
	}

	return nil
}

// Workers is not required by pop and may be deleted
type Workers []Worker

// String is not required by pop and may be deleted
func (t Workers) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (t *Worker) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.Validate(
		&validators.StringIsPresent{Field: string(t.Address), Name: "Address"},
	), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (t *Worker) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (t *Worker) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
