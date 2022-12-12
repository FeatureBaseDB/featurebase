package queryer

import (
	"context"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/schemar"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ pilosa.SchemaAPI = (*qualifiedSchemaAPI)(nil)

// qualifiedSchemaAPI is a wrapper around schemaAPI. It is initialized with a
// TableQualifier, and it uses this qualifer to convert between, for example,
// FeatureBase index name (a string) and TableKey. It requires a Schemar to do
// that lookup/conversion.
type qualifiedSchemaAPI struct {
	qual    dax.TableQualifier
	schemar schemar.Schemar
}

func NewQualifiedSchemaAPI(qual dax.TableQualifier, schemar schemar.Schemar) *qualifiedSchemaAPI {
	return &qualifiedSchemaAPI{
		qual:    qual,
		schemar: schemar,
	}
}

func (s *qualifiedSchemaAPI) TableByName(ctx context.Context, tname dax.TableName) (*dax.Table, error) {
	qtid, err := s.schemar.TableID(ctx, s.qual, tname)
	if err != nil {
		return nil, errors.Wrapf(err, "getting table id: (%s) %s", s.qual, tname)
	}

	qtbl, err := s.schemar.Table(ctx, qtid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting table: %s", qtid)
	}

	return &qtbl.Table, nil
}

func (s *qualifiedSchemaAPI) TableByID(ctx context.Context, tid dax.TableID) (*dax.Table, error) {
	qtid := dax.NewQualifiedTableID(s.qual, tid)

	qtbl, err := s.schemar.Table(ctx, qtid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting table: %s", qtid)
	}

	return &qtbl.Table, nil
}

func (s *qualifiedSchemaAPI) Tables(ctx context.Context) ([]*dax.Table, error) {
	qtbls, err := s.schemar.Tables(ctx, s.qual)
	if err != nil {
		return nil, errors.Wrap(err, "getting tables")
	}

	tbls := make([]*dax.Table, 0, len(qtbls))
	for _, qtbl := range qtbls {
		tbls = append(tbls, &qtbl.Table)
	}

	return tbls, nil
}

func (s *qualifiedSchemaAPI) CreateTable(ctx context.Context, tbl *dax.Table) error {
	qtbl := dax.NewQualifiedTable(s.qual, tbl)
	return s.schemar.CreateTable(ctx, qtbl)
}

func (s *qualifiedSchemaAPI) CreateField(ctx context.Context, tname dax.TableName, fld *dax.Field) error {
	qtid, err := s.schemar.TableID(ctx, s.qual, tname)
	if err != nil {
		return errors.Wrapf(err, "getting table id: (%s) %s", s.qual, tname)
	}

	return s.schemar.CreateField(ctx, qtid, fld)
}

func (s *qualifiedSchemaAPI) DeleteTable(ctx context.Context, tname dax.TableName) error {
	qtid, err := s.schemar.TableID(ctx, s.qual, tname)
	if err != nil {
		return errors.Wrapf(err, "getting table id: (%s) %s", s.qual, tname)
	}

	return s.schemar.DropTable(ctx, qtid)
}

func (s *qualifiedSchemaAPI) DeleteField(ctx context.Context, tname dax.TableName, fname dax.FieldName) error {
	qtid, err := s.schemar.TableID(ctx, s.qual, tname)
	if err != nil {
		return errors.Wrapf(err, "getting table id: (%s) %s", s.qual, tname)
	}

	return s.schemar.DropField(ctx, qtid, fname)
}
