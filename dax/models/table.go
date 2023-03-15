package models

import (
	"encoding/json"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/gobuffalo/pop/v6"
	"github.com/gobuffalo/validate/v3"
	"github.com/gobuffalo/validate/v3/validators"
)

// Table is used by pop to map your tables database table to your go code.
type Table struct {
	// ID will store the dax.Table.Key(), but must be string type due to pop's nonsense
	ID             string             `json:"id" db:"id"`
	Name           dax.TableName      `json:"name" db:"name"`
	Owner          string             `json:"owner" db:"owner"`
	UpdatedBy      string             `json:"updated_by" db:"updated_by"`
	Database       *Database          `json:"database" belongs_to:"database"`
	DatabaseID     string             `json:"database_id" db:"database_id"`
	OrganizationID dax.OrganizationID `json:"organization_id" db:"organization_id"`
	Description    string             `json:"description" db:"description"`
	PartitionN     int                `json:"partition_n" db:"partition_n"`
	Columns        Columns            `json:"columns" has_many:"columns" order_by:"created_at asc"`
	CreatedAt      time.Time          `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at" db:"updated_at"`
}

// String is not required by pop and may be deleted
func (t *Table) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}

// Tables is not required by pop and may be deleted
type Tables []*Table

// String is not required by pop and may be deleted
func (t Tables) String() string {
	jt, _ := json.MarshalIndent(t, " ", " ") //nolint:errchkjson
	return string(jt)
}

// Validate gets run every time you call a "pop.Validate*" (pop.ValidateAndSave, pop.ValidateAndCreate, pop.ValidateAndUpdate) method.
// This method is not required and may be deleted.
func (t *Table) Validate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.Validate(
		&validators.StringIsPresent{Field: string(t.Name), Name: "Name"},
		&validators.StringIsPresent{Field: t.Owner, Name: "Owner"},
		&validators.StringIsPresent{Field: t.UpdatedBy, Name: "UpdatedBy"},
		&validators.StringIsPresent{Field: t.Description, Name: "Description"},
		&validators.IntIsPresent{Field: t.PartitionN, Name: "PartitionN"},
	), nil
}

// ValidateCreate gets run every time you call "pop.ValidateAndCreate" method.
// This method is not required and may be deleted.
func (t *Table) ValidateCreate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}

// ValidateUpdate gets run every time you call "pop.ValidateAndUpdate" method.
// This method is not required and may be deleted.
func (t *Table) ValidateUpdate(tx *pop.Connection) (*validate.Errors, error) {
	return validate.NewErrors(), nil
}
