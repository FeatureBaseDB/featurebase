// Package test include external test apps, helper functions, and test data.
package test

import (
	"testing"

	"github.com/molecula/featurebase/v3/dax"
)

// TestQualifiedTable is a test helper function for creating a table based on a
// general configuration. This function creates a Table with a random TableID.
// If you need to specify the TableID yourself, use the TestQualifiedTableWithID
// function.
func TestQualifiedTable(t *testing.T, qual dax.TableQualifier, name dax.TableName, partitionN int, keyed bool) *dax.QualifiedTable {
	t.Helper()

	var pkFieldType dax.BaseType
	if keyed {
		pkFieldType = dax.BaseTypeString
	} else {
		pkFieldType = dax.BaseTypeID
	}

	tbl := dax.NewTable(name)
	tbl.CreateID()
	tbl.PartitionN = partitionN
	tbl.Fields = []*dax.Field{
		{
			Name: dax.PrimaryKeyFieldName,
			Type: pkFieldType,
		},
	}

	return dax.NewQualifiedTable(
		qual,
		tbl,
	)
}

// TestQualifiedTableWithID is a test helper function for creating a table based
// on a general configuration, and having the specified TableID.
func TestQualifiedTableWithID(t *testing.T, qual dax.TableQualifier, id string, name dax.TableName, partitionN int, keyed bool) *dax.QualifiedTable {
	t.Helper()

	var pkFieldType dax.BaseType
	if keyed {
		pkFieldType = dax.BaseTypeString
	} else {
		pkFieldType = dax.BaseTypeID
	}

	tbl := &dax.Table{
		ID:   dax.TableID(id),
		Name: name,
	}

	tbl.PartitionN = partitionN
	tbl.Fields = []*dax.Field{
		{
			Name: dax.PrimaryKeyFieldName,
			Type: pkFieldType,
		},
	}

	return dax.NewQualifiedTable(
		qual,
		tbl,
	)
}
