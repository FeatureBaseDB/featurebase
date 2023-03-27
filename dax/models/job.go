package models

import (
	"encoding/json"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/gobuffalo/nulls"
	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
	"github.com/gobuffalo/validate/v3/validators"
	"github.com/gofrs/uuid"
)

// Job represents a job which can be assigned to a worker or free (unassigned).
type Job struct {
	ID         uuid.UUID      `json:"id" db:"id"`
	Name       dax.Job        `json:"name" db:"name"`
	Role       dax.RoleType   `json:"role" db:"role"`
	DatabaseID dax.DatabaseID `json:"database_id" db:"database_id"`
	WorkerID   nulls.UUID     `json:"-" db:"worker_id"`
	Worker     *Worker        `json:"worker" db:"-" belongs_to:"worker"`
	CreatedAt  time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (t *Job) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}

// Jobs is not required by pop and may be deleted
type Jobs []Job

// String is not required by pop and may be deleted
func (t Jobs) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}

// Contains returns true if j is in Jobs.
func (t Jobs) Contains(j dax.Job) bool {
	for i := range t {
		if t[i].Name == j {
			return true
		}
	}
	return false
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (t *Job) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.Validate(
		&validators.StringIsPresent{Field: string(t.Name), Name: "Name"},
	), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (t *Job) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (t *Job) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
