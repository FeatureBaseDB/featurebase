package queryer

import (
	"context"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/mds/schemar"
	"github.com/featurebasedb/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ pilosa.SchemaInfoAPI = (*schemaInfoAPI)(nil)

type schemaInfoAPI struct {
	schemar schemar.Schemar
}

func NewSchemaInfoAPI(schemar schemar.Schemar) *schemaInfoAPI {
	return &schemaInfoAPI{
		schemar: schemar,
	}
}

func (a *schemaInfoAPI) IndexInfo(ctx context.Context, indexName string) (*pilosa.IndexInfo, error) {
	qtid := dax.TableKey(indexName).QualifiedTableID()
	tbl, err := a.schemar.Table(ctx, qtid)
	if err != nil {
		return nil, errors.Wrap(err, "getting table for indexinfo")
	}

	return daxTableToFeaturebaseIndexInfo(tbl, false)
}

func (a *schemaInfoAPI) FieldInfo(ctx context.Context, indexName, fieldName string) (*pilosa.FieldInfo, error) {
	qtid := dax.TableKey(indexName).QualifiedTableID()
	tbl, err := a.schemar.Table(ctx, qtid)
	fldName := dax.FieldName(fieldName)

	if err != nil {
		return nil, errors.Wrap(err, "getting table for fieldinfo")
	}

	fld, ok := tbl.Field(dax.FieldName(fieldName))
	if !ok {
		return nil, dax.NewErrFieldDoesNotExist(fldName)
	}

	return pilosa.FieldToFieldInfo(fld), nil
}

// TODO(tlt): try to get rid of this in favor of pilosa.TableToIndexInfo.
// daxTableToFeaturebaseIndexInfo converts a dax.Table to a
// featurebase.IndexInfo. If useName is true, the IndexInfo.Name value will
// be set to the qualified table name. Otherwise it will be set to the table key.
func daxTableToFeaturebaseIndexInfo(qtbl *dax.QualifiedTable, useName bool) (*pilosa.IndexInfo, error) {
	name := string(qtbl.Key())
	if useName {
		name = string(qtbl.Name)
	}
	ii := &pilosa.IndexInfo{
		Name:      name,
		CreatedAt: 0,
		Options: pilosa.IndexOptions{
			Keys:           qtbl.StringKeys(),
			TrackExistence: true,
		},
		ShardWidth: pilosa.ShardWidth,
	}

	// fields
	fields := make([]*pilosa.FieldInfo, len(qtbl.Fields))
	for i := range qtbl.Fields {
		fields[i] = pilosa.FieldToFieldInfo(qtbl.Fields[i])
	}
	ii.Fields = fields

	return ii, nil
}
