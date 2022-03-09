package schemar

import (
	"fmt"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/errors"
)

const (
	ErrCodeTableIDInvalid    errors.Code = "TableIDInvalid"
	ErrCodeTableNameInvalid  errors.Code = "TableNameInvalid"
	ErrCodeInvalidPrimaryKey errors.Code = "InvalidPrimaryKey"

	ErrCodeFieldNameInvalid errors.Code = "FieldNameInvalid"
)

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
