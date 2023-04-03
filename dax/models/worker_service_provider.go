package models

import (
	"encoding/json"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
	"github.com/gobuffalo/validate/v3/validators"
)

// WorkerServiceProvider represents an entity which has registered with the
// Controller as being able to provide Services (a Service being an
// isolated container for compute workers or queryer workers).
type WorkerServiceProvider struct {
	ID            string      `json:"id" db:"id"`
	RoleCompute   bool        `json:"role_compute" db:"role_compute"`
	RoleTranslate bool        `json:"role_translate" db:"role_translate"`
	Address       dax.Address `json:"address" db:"address"`
	Description   string      `json:"description" db:"description"`
	CreatedAt     time.Time   `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (d WorkerServiceProvider) String() string {
	jd, _ := json.MarshalIndent(d, " ", " ") //nolint:errchkjson
	return string(jd)
}

// WorkerServiceProviders is not required by pop and may be deleted
type WorkerServiceProviders []WorkerServiceProvider

// String is not required by pop and may be deleted
func (d WorkerServiceProviders) String() string {
	jd, _ := json.Marshal(d) //nolint:errchkjson
	return string(jd)
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (d *WorkerServiceProvider) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.Validate(
		&validators.StringIsPresent{Field: string(d.ID), Name: "ID"},
		&validators.StringIsPresent{Field: string(d.Address), Name: "Address"},
	), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (d *WorkerServiceProvider) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (d *WorkerServiceProvider) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
