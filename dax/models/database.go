package models

import (
	"encoding/json"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
)

// Database is used by pop to map your databases database table to your go code.
type Database struct {
	// should be DatabaseID, but pop doesn't allow ID to be a custom type
	ID             string           `json:"id" db:"id"`
	Name           dax.DatabaseName `json:"name" db:"name"`
	WorkersMin     int              `json:"workers_min" db:"workers_min"`
	WorkersMax     int              `json:"workers_max" db:"workers_max"`
	Description    string           `json:"description" db:"description"`
	Owner          string           `json:"owner" db:"owner"`
	UpdatedBy      string           `json:"updated_by" db:"updated_by"`
	Tables         Tables           `json:"tables" has_many:"tables"`
	Organization   *Organization    `json:"organization" belongs_to:"organization"`
	OrganizationID string           `json:"organization_id" db:"organization_id"`
	CreatedAt      time.Time        `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (d Database) String() string {
	jd, _ := json.MarshalIndent(d, " ", " ") //nolint:errchkjson
	return string(jd)
}

// Databases is not required by pop and may be deleted
type Databases []Database

// String is not required by pop and may be deleted
func (d Databases) String() string {
	jd, _ := json.Marshal(d) //nolint:errchkjson
	return string(jd)
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (d *Database) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (d *Database) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (d *Database) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
