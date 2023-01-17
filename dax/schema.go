package dax

import "context"

// Schemar is similar to the pilosa.SchemaAPI interface, but it takes
// QualifiedDatabaseIDs into account. Note that it is also similar to the
// schemar.Schemar interface, but that is used internally, typically within the
// Controller, and it takes Transactions rather than a Context, because its
// methods are assumed to be used as part of a larger request.
// TODO(tlt): clean up the mds/controller/schemar Schemar interface confusion.
type Schemar interface {
	//////////////////////////////////////////////////////////////////////////
	// Database methods
	//////////////////////////////////////////////////////////////////////////

	CreateDatabase(context.Context, *QualifiedDatabase) error
	// DropDatabase(context.Context, QualifiedDatabaseID) error

	// DatabaseByName(ctx context.Context, orgID OrganizationID, dbname DatabaseName) (*QualifiedDatabase, error)
	DatabaseByID(ctx context.Context, qdbid QualifiedDatabaseID) (*QualifiedDatabase, error)

	// // Databases returns a list of databases. If the OrganizationID is empty,
	// // all databases will be returned. If greater than zero database IDs are
	// // passed in the second argument, only databases matching those IDs will be
	// // returned.
	// Databases(context.Context, OrganizationID, ...DatabaseID) ([]*QualifiedDatabase, error)

	// SetDatabaseOptions(context.Context, QualifiedDatabaseID, DatabaseOptions) error

	//////////////////////////////////////////////////////////////////////////
	// Table methods
	//////////////////////////////////////////////////////////////////////////

	CreateTable(ctx context.Context, qtbl *QualifiedTable) error
	DropTable(ctx context.Context, qtid QualifiedTableID) error

	TableByName(ctx context.Context, qdbid QualifiedDatabaseID, tname TableName) (*QualifiedTable, error)
	TableByID(ctx context.Context, qtid QualifiedTableID) (*QualifiedTable, error)

	// Tables returns a list of tables. If the qualifiers DatabaseID is empty,
	// all tables in the org will be returned. If the OrganizationID is empty,
	// all tables will be returned. If both are populated, only tables in that
	// database will be returned. If greater than zero table IDs are passed in
	// the third argument, only tables matching those IDs will be returned.
	Tables(ctx context.Context, qdbid QualifiedDatabaseID, tids ...TableID) ([]*QualifiedTable, error)

	//////////////////////////////////////////////////////////////////////////
	// Field methods
	//////////////////////////////////////////////////////////////////////////

	CreateField(ctx context.Context, qtid QualifiedTableID, fld *Field) error
	DropField(ctx context.Context, qtid QualifiedTableID, fname FieldName) error
}

//////////////////////////////////////////////

// Ensure type implements interface.
var _ Schemar = &NopSchemar{}

// NopSchemar is a no-op implementation of the Schemar interface.
type NopSchemar struct{}

func NewNopSchemar() *NopSchemar {
	return &NopSchemar{}
}

func (s *NopSchemar) CreateDatabase(context.Context, *QualifiedDatabase) error {
	return nil
}
func (s *NopSchemar) DatabaseByID(ctx context.Context, qdbid QualifiedDatabaseID) (*QualifiedDatabase, error) {
	return nil, nil
}
func (s *NopSchemar) TableByName(context.Context, QualifiedDatabaseID, TableName) (*QualifiedTable, error) {
	return nil, nil
}
func (s *NopSchemar) TableByID(ctx context.Context, qtid QualifiedTableID) (*QualifiedTable, error) {
	return nil, nil
}
func (s *NopSchemar) Tables(ctx context.Context, qdbid QualifiedDatabaseID, tids ...TableID) ([]*QualifiedTable, error) {
	return nil, nil
}
func (s *NopSchemar) CreateTable(ctx context.Context, qtbl *QualifiedTable) error {
	return nil
}
func (s *NopSchemar) DropTable(ctx context.Context, qtid QualifiedTableID) error {
	return nil
}
func (s *NopSchemar) CreateField(ctx context.Context, qtid QualifiedTableID, fld *Field) error {
	return nil
}
func (s *NopSchemar) DropField(ctx context.Context, qtid QualifiedTableID, fld FieldName) error {
	return nil
}
