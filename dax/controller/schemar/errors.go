package schemar

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

const (
	ErrCodeDatabaseIDInvalid   errors.Code = "DatabaseIDInvalid"
	ErrCodeDatabaseNameInvalid errors.Code = "DatabaseNameInvalid"

	ErrCodeTableIDInvalid    errors.Code = "TableIDInvalid"
	ErrCodeTableNameInvalid  errors.Code = "TableNameInvalid"
	ErrCodeInvalidPrimaryKey errors.Code = "InvalidPrimaryKey"

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
		fmt.Sprintf("invalid index or field name %s, must match [a-z][a-z0-9Θ_-]* and contain at most 230 characters", databaseName),
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
