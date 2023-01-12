package dax

import "context"

// Schemar is similar to the pilosa.SchemaAPI interface, but it takes
// TableQualifiers into account.
type Schemar interface {
	TableByName(ctx context.Context, qual TableQualifier, tname TableName) (*QualifiedTable, error)
	TableByID(ctx context.Context, qtid QualifiedTableID) (*QualifiedTable, error)
	Tables(ctx context.Context, qual TableQualifier, tids ...TableID) ([]*QualifiedTable, error)

	CreateTable(ctx context.Context, qtbl *QualifiedTable) error
	CreateField(ctx context.Context, qtid QualifiedTableID, fld *Field) error

	DropTable(ctx context.Context, qtid QualifiedTableID) error
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

func (s *NopSchemar) TableByName(context.Context, TableQualifier, TableName) (*QualifiedTable, error) {
	return nil, nil
}
func (s *NopSchemar) TableByID(ctx context.Context, qtid QualifiedTableID) (*QualifiedTable, error) {
	return nil, nil
}
func (s *NopSchemar) Tables(ctx context.Context, qual TableQualifier, tids ...TableID) ([]*QualifiedTable, error) {
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
