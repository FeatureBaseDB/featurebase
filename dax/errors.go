package dax

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/errors"
)

const (
	ErrOrganizationIDDoesNotExist errors.Code = "OrganizationIDDoesNotExist"

	ErrDatabaseIDExists         errors.Code = "DatabaseIDExists"
	ErrDatabaseIDDoesNotExist   errors.Code = "DatabaseIDDoesNotExist"
	ErrDatabaseNameDoesNotExist errors.Code = "DatabaseNameDoesNotExist"

	ErrTableIDExists         errors.Code = "TableIDExists"
	ErrTableKeyExists        errors.Code = "TableKeyExists"
	ErrTableNameExists       errors.Code = "TableNameExists"
	ErrTableIDDoesNotExist   errors.Code = "TableIDDoesNotExist"
	ErrTableKeyDoesNotExist  errors.Code = "TableKeyDoesNotExist"
	ErrTableNameDoesNotExist errors.Code = "TableNameDoesNotExist"

	ErrFieldExists       errors.Code = "FieldExists"
	ErrFieldDoesNotExist errors.Code = "FieldDoesNotExist"

	ErrInvalidTransaction errors.Code = "InvalidTransaction"

	ErrUnimplemented errors.Code = "Unimplemented"
)

// The following are helper functions for constructing coded errors containing
// relevant information about the specific error.

func NewErrOrganizationIDDoesNotExist(orgID OrganizationID) error {
	return errors.New(
		ErrOrganizationIDDoesNotExist,
		fmt.Sprintf("Organization ID '%s' does not exist", orgID),
	)
}

func NewErrDatabaseIDExists(qdbid QualifiedDatabaseID) error {
	return errors.New(
		ErrDatabaseIDExists,
		fmt.Sprintf("database ID '%s' already exists", qdbid),
	)
}

func NewErrDatabaseIDDoesNotExist(qdbid QualifiedDatabaseID) error {
	return errors.New(
		ErrDatabaseIDDoesNotExist,
		fmt.Sprintf("database ID '%s' does not exist", qdbid),
	)
}

func NewErrDatabaseNameDoesNotExist(dbName DatabaseName) error {
	return errors.New(
		ErrDatabaseNameDoesNotExist,
		fmt.Sprintf("database name '%s' does not exist", dbName),
	)
}

func NewErrTableIDDoesNotExist(qtid QualifiedTableID) error {
	return errors.New(
		ErrTableIDDoesNotExist,
		fmt.Sprintf("table ID '%s' does not exist", qtid),
	)
}

func NewErrTableKeyDoesNotExist(tkey TableKey) error {
	return errors.New(
		ErrTableKeyDoesNotExist,
		fmt.Sprintf("table key '%s' does not exist", tkey),
	)
}

func NewErrTableNameDoesNotExist(tableName TableName) error {
	return errors.New(
		ErrTableNameDoesNotExist,
		fmt.Sprintf("table name '%s' does not exist", tableName),
	)
}

func NewErrTableIDExists(qtid QualifiedTableID) error {
	return errors.New(
		ErrTableIDExists,
		fmt.Sprintf("table ID '%s' already exists", qtid),
	)
}

func NewErrTableKeyExists(tkey TableKey) error {
	return errors.New(
		ErrTableKeyExists,
		fmt.Sprintf("table key '%s' already exists", tkey),
	)
}

func NewErrTableNameExists(tableName TableName) error {
	return errors.New(
		ErrTableNameExists,
		fmt.Sprintf("table name '%s' already exists", tableName),
	)
}

func NewErrFieldDoesNotExist(fieldName FieldName) error {
	return errors.New(
		ErrFieldDoesNotExist,
		fmt.Sprintf("field '%s' does not exist", fieldName),
	)
}

func NewErrFieldExists(fieldName FieldName) error {
	return errors.New(
		ErrFieldExists,
		fmt.Sprintf("field '%s' already exists", fieldName),
	)
}

func NewErrInvalidTransaction(txType string) error {
	return errors.New(
		ErrInvalidTransaction,
		fmt.Sprintf("tx is not expected type: '%s'", txType),
	)
}
