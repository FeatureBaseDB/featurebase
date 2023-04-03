package models

import (
	"encoding/json"
	"time"

	"github.com/gobuffalo/nulls"
	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
	"github.com/gobuffalo/validate/v3/validators"
)

// WorkerService represents an entity which has registered with the
// Controller as being able to provide WorkerServices (a WorkerService being an
// isolated container for compute workers or queryer workers).
type WorkerService struct {
	ID                      string       `json:"id" db:"id"`
	WorkerServiceProviderID string       `json:"worker_service_provider_id" db:"worker_service_provider_id"`
	DatabaseID              nulls.String `json:"database_id" db:"database_id"`
	RoleCompute             bool         `json:"role_compute" db:"role_compute"`
	RoleTranslate           bool         `json:"role_translate" db:"role_translate"`
	WorkersMin              int          `json:"workers_min" db:"workers_min"`
	WorkersMax              int          `json:"workers_max" db:"workers_max"`
	CreatedAt               time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt               time.Time    `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (d WorkerService) String() string {
	jd, _ := json.MarshalIndent(d, " ", " ") //nolint:errchkjson
	return string(jd)
}

// WorkerServices is not required by pop and may be deleted
type WorkerServices []WorkerService

// String is not required by pop and may be deleted
func (d WorkerServices) String() string {
	jd, _ := json.Marshal(d) //nolint:errchkjson
	return string(jd)
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (d *WorkerService) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.Validate(
		&validators.StringIsPresent{Field: string(d.ID), Name: "ID"},
		&validators.StringIsPresent{Field: d.WorkerServiceProviderID, Name: "WorkerServiceProviderID"},
	), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (d *WorkerService) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (d *WorkerService) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
