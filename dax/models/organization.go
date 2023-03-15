package models

import (
	"encoding/json"
	"time"

	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
)

// Organization is used by pop to map your organizations database table to your go code.
type Organization struct {
	// Would love for this to be OrganizationID, but pop doesn't allow it for primary keys
	ID        string    `json:"id" db:"id"`
	Databases Databases `json:"databases" db:"-" has_many:"databases" order_by:"created_at asc"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (o *Organization) String() string {
	jo, _ := json.MarshalIndent(o, " ", " ") //nolint:errchkjson
	return string(jo)
}

// Organizations is not required by pop and may be deleted
type Organizations []Organization

// String is not required by pop and may be deleted
func (o Organizations) String() string {
	jo, _ := json.MarshalIndent(o, " ", " ") //nolint:errchkjson
	return string(jo)
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (o *Organization) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (o *Organization) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (o *Organization) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
