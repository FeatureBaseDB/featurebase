package schemar

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

const (
	ErrCodeDatabaseIDInvalid   errors.Code = "DatabaseIDInvalid"
	ErrCodeDatabaseNameInvalid errors.Code = "DatabaseNameInvalid"
	ErrCodeDatabaseNameExists  errors.Code = "DatabaseNameExists"

	ErrCodeTableIDInvalid    errors.Code = "TableIDInvalid"
	ErrCodeTableNameInvalid  errors.Code = "TableNameInvalid"
	ErrCodeInvalidPrimaryKey errors.Code = "InvalidPrimaryKey"
	ErrCodeTableNameExists   errors.Code = "TableNameExists"

	ErrCodeFieldNameInvalid errors.Code = "FieldNameInvalid"
)

func NewErrDatabaseIDInvalid(databaseID dax.DatabaseID) error {
	return errors.New(
		ErrCodeDatabaseIDInvalid,
		fmt.Sprintf("database ID '%s' is invalid", databaseID),
	)
}

func NewErrDatabaseNameInvalid(databaseName dax.DatabaseName) error {
	return errors.New(
		ErrCodeDatabaseNameInvalid,
		fmt.Sprintf("database name '%s' is invalid", databaseName),
	)
}

func NewErrTableIDInvalid(tableID dax.TableID) error {
	return errors.New(
		ErrCodeTableIDInvalid,
		fmt.Sprintf("table ID '%s' is invalid", tableID),
	)
}

func NewErrTableNameInvalid(tableName dax.TableName) error {
	return errors.New(
		ErrCodeTableNameInvalid,
		fmt.Sprintf("table name '%s' is invalid", tableName),
	)
}

func NewErrInvalidPrimaryKey() error {
	return errors.New(
		ErrCodeInvalidPrimaryKey,
		"invalid primary key",
	)
}

func NewErrFieldNameInvalid(fieldName dax.FieldName) error {
	return errors.New(
		ErrCodeFieldNameInvalid,
		fmt.Sprintf("field name '%s' is invalid", fieldName),
	)
}

func NewErrDatabaseNameExists(databaseName dax.DatabaseName) error {
	return errors.New(
		ErrCodeDatabaseNameExists,
		fmt.Sprintf("database name %s already exists", databaseName),
	)
}

func NewErrTableNameExists(tableName dax.TableName) error {
	return errors.New(
		ErrCodeTableNameExists,
		fmt.Sprintf("table name %s already exists", tableName),
	)
}
