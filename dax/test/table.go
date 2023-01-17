// Package test include external test apps, helper functions, and test data.
package test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
)

// TestQualifiedTable is a test helper function for creating a table based on a
// general configuration. This function creates a Table with a random TableID.
// If you need to specify the TableID yourself, use the TestQualifiedTableWithID
// function.
func TestQualifiedTable(t *testing.T, qdbid dax.QualifiedDatabaseID, name dax.TableName, partitionN int, keyed bool) *dax.QualifiedTable {
	t.Helper()

	var pkFieldType dax.BaseType
	if keyed {
		pkFieldType = dax.BaseTypeString
	} else {
		pkFieldType = dax.BaseTypeID
	}

	tbl := dax.NewTable(name)
	tbl.PartitionN = partitionN
	tbl.Fields = []*dax.Field{
		{
			Name: dax.PrimaryKeyFieldName,
			Type: pkFieldType,
		},
	}

	return dax.NewQualifiedTable(
		qdbid,
		tbl,
	)
}

// TestQualifiedDatabaseWithID is a test helper function for creating a database
// based on a general configuration, and having the specified DatabaseID.
func TestQualifiedDatabaseWithID(t *testing.T, orgID dax.OrganizationID, id dax.DatabaseID, name dax.DatabaseName, opts dax.DatabaseOptions) *dax.QualifiedDatabase {
	t.Helper()

	db := dax.Database{
		ID:      dax.DatabaseID(id),
		Name:    name,
		Options: opts,
	}

	qdb := &dax.QualifiedDatabase{
		OrganizationID: orgID,
		Database:       db,
	}

	return qdb
}

// TestQualifiedTableWithID is a test helper function for creating a table based
// on a general configuration, and having the specified TableID.
func TestQualifiedTableWithID(t *testing.T, qdbid dax.QualifiedDatabaseID, id string, name dax.TableName, partitionN int, keyed bool) *dax.QualifiedTable {
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
		qdbid,
		tbl,
	)
}
