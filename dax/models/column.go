package models

import (
	"encoding/json"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
	"github.com/gobuffalo/validate/v3/validators"
	"github.com/gofrs/uuid"
)

// Column is used by pop to map your columns database table to your go code.
type Column struct {
	ID          uuid.UUID     `json:"id" db:"id"`
	Name        dax.FieldName `json:"name" db:"name"`
	Type        dax.BaseType  `json:"type" db:"type"`
	TableID     string        `json:"table_id" db:"table_id"`
	Constraints string        `json:"constraints" db:"constraints"`
	CreatedAt   time.Time     `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (c *Column) String() string {
	jc, _ := json.MarshalIndent(c, " ", " ")
	return string(jc)
}

// Columns is not required by pop and may be deleted
type Columns []Column

// String is not required by pop and may be deleted
func (c Columns) String() string {
	jc, _ := json.MarshalIndent(c, " ", " ")
	return string(jc)
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (c *Column) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.Validate(
		&validators.StringIsPresent{Field: string(c.Name), Name: "Name"},
		&validators.StringIsPresent{Field: string(c.Type), Name: "Type"},
		&validators.StringIsPresent{Field: c.Constraints, Name: "Constraints"},
	), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (c *Column) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (c *Column) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
