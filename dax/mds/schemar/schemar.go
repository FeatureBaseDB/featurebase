// Package schemar provides the core Schemar interface.
package schemar

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
)

type Schemar interface {
	CreateTable(context.Context, *dax.QualifiedTable) error
	DropTable(context.Context, dax.QualifiedTableID) error
	CreateField(context.Context, dax.QualifiedTableID, *dax.Field) error
	DropField(context.Context, dax.QualifiedTableID, dax.FieldName) error
	Table(context.Context, dax.QualifiedTableID) (*dax.QualifiedTable, error)
	Tables(context.Context, dax.TableQualifier, ...dax.TableID) ([]*dax.QualifiedTable, error)

	// TableID is a reverse-lookup method to get the TableID for a given
	// qualified TableName.
	TableID(context.Context, dax.TableQualifier, dax.TableName) (dax.QualifiedTableID, error)
}

//////////////////////////////////////////////

// Ensure type implements interface.
var _ Schemar = &NopSchemar{}

// NopSchemar is a no-op implementation of the Schemar interface.
type NopSchemar struct{}

func NewNopSchemar() *NopSchemar {
	return &NopSchemar{}
}

func (s *NopSchemar) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error { return nil }
func (s *NopSchemar) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	return nil
}
func (s *NopSchemar) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	return nil
}
func (s *NopSchemar) DropField(ctx context.Context, qtid dax.QualifiedTableID, fld dax.FieldName) error {
	return nil
}
func (s *NopSchemar) Table(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	return nil, nil
}
func (s *NopSchemar) Tables(ctx context.Context, qual dax.TableQualifier, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	return []*dax.QualifiedTable{}, nil
}

func (s *NopSchemar) TableID(context.Context, dax.TableQualifier, dax.TableName) (dax.QualifiedTableID, error) {
	return dax.QualifiedTableID{}, nil
}
