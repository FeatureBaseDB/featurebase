// Package schemar provides the core Schemar interface.
package schemar

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
)

type Schemar interface {
	CreateDatabase(dax.Transaction, *dax.QualifiedDatabase) error
	DropDatabase(dax.Transaction, dax.QualifiedDatabaseID) error
	DatabaseByID(dax.Transaction, dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error)

	SetDatabaseOptions(dax.Transaction, dax.QualifiedDatabaseID, dax.DatabaseOptions) error

	// Databases returns a list of databases. If the OrganizationID is empty,
	// all databases will be returned. If greater than zero database IDs are
	// passed in the second argument, only databases matching those IDs will be
	// returned.
	Databases(dax.Transaction, dax.OrganizationID, ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error)

	CreateTable(dax.Transaction, *dax.QualifiedTable) error
	DropTable(dax.Transaction, dax.QualifiedTableID) error
	CreateField(dax.Transaction, dax.QualifiedTableID, *dax.Field) error
	DropField(dax.Transaction, dax.QualifiedTableID, dax.FieldName) error
	Table(dax.Transaction, dax.QualifiedTableID) (*dax.QualifiedTable, error)

	// Tables returns a list of tables. If the qualifiers DatabaseID is empty,
	// all tables in the org will be returned. If the OrganizationID is empty,
	// all tables will be returned. If both are populated, only tables in that
	// database will be returned. If greater than zero table IDs are passed in
	// the third argument, only tables matching those IDs will be returned.
	Tables(dax.Transaction, dax.QualifiedDatabaseID, ...dax.TableID) ([]*dax.QualifiedTable, error)

	// TableID is a reverse-lookup method to get the TableID for a given
	// qualified TableName.
	TableID(dax.Transaction, dax.QualifiedDatabaseID, dax.TableName) (dax.QualifiedTableID, error)
}

//////////////////////////////////////////////

// Ensure type implements interface.
var _ Schemar = &NopSchemar{}

// NopSchemar is a no-op implementation of the Schemar interface.
type NopSchemar struct{}

func NewNopSchemar() *NopSchemar {
	return &NopSchemar{}
}

func (s *NopSchemar) CreateDatabase(tx dax.Transaction, qtbl *dax.QualifiedDatabase) error {
	return nil
}

func (s *NopSchemar) DropDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) error {
	return nil
}

func (s *NopSchemar) DatabaseByID(dax.Transaction, dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error) {
	return nil, nil
}

func (s *NopSchemar) SetDatabaseOptions(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, opts dax.DatabaseOptions) error {
	return nil
}

func (s *NopSchemar) Databases(dax.Transaction, dax.OrganizationID, ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error) {
	return nil, nil
}

func (s *NopSchemar) CreateTable(tx dax.Transaction, qtbl *dax.QualifiedTable) error {
	return nil
}

func (s *NopSchemar) DropTable(tx dax.Transaction, qtid dax.QualifiedTableID) error {
	return nil
}

func (s *NopSchemar) CreateField(tx dax.Transaction, qtid dax.QualifiedTableID, fld *dax.Field) error {
	return nil
}

func (s *NopSchemar) DropField(tx dax.Transaction, qtid dax.QualifiedTableID, fld dax.FieldName) error {
	return nil
}

func (s *NopSchemar) Table(tx dax.Transaction, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	return nil, nil
}

func (s *NopSchemar) Tables(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	return []*dax.QualifiedTable{}, nil
}

func (s *NopSchemar) TableID(dax.Transaction, dax.QualifiedDatabaseID, dax.TableName) (dax.QualifiedTableID, error) {
	return dax.QualifiedTableID{}, nil
}
