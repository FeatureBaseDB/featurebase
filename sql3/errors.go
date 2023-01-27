package sql3

import (
	"fmt"
	"runtime"

	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

const (
	ErrInternal         errors.Code = "ErrInternal"
	ErrUnsupported      errors.Code = "ErrUnsupported"
	ErrCacheKeyNotFound errors.Code = "ErrCacheKeyNotFound"

	ErrDuplicateColumn   errors.Code = "ErrDuplicateColumn"
	ErrUnknownType       errors.Code = "ErrUnknownType"
	ErrUnknownIdentifier errors.Code = "ErrUnknownIdentifier"

	ErrTypeIncompatibleWithBitwiseOperator       errors.Code = "ErrTypeIncompatibleWithBitwiseOperator"
	ErrTypeIncompatibleWithLogicalOperator       errors.Code = "ErrTypeIncompatibleWithLogicalOperator"
	ErrTypeIncompatibleWithEqualityOperator      errors.Code = "ErrTypeIncompatibleWithEqualityOperator"
	ErrTypeIncompatibleWithComparisonOperator    errors.Code = "ErrTypeIncompatibleWithComparisonOperator"
	ErrTypeIncompatibleWithArithmeticOperator    errors.Code = "ErrTypeIncompatibleWithArithmeticOperator"
	ErrTypeIncompatibleWithConcatOperator        errors.Code = "ErrTypeIncompatibleWithConcatOperator"
	ErrTypeIncompatibleWithLikeOperator          errors.Code = "ErrTypeIncompatibleWithLikeOperator"
	ErrTypeIncompatibleWithBetweenOperator       errors.Code = "ErrTypeIncompatibleWithBetweenOperator"
	ErrTypeCannotBeUsedAsRangeSubscript          errors.Code = "ErrTypeCannotBeUsedAsRangeSubscript"
	ErrTypesAreNotEquatable                      errors.Code = "ErrTypesAreNotEquatable"
	ErrTypeMismatch                              errors.Code = "ErrTypeMismatch"
	ErrIncompatibleTypesForRangeSubscripts       errors.Code = "ErrIncompatibleTypesForRangeSubscripts"
	ErrExpressionListExpected                    errors.Code = "ErrExpressionListExpected"
	ErrBooleanExpressionExpected                 errors.Code = "ErrBooleanExpressionExpected"
	ErrIntExpressionExpected                     errors.Code = "ErrIntExpressionExpected"
	ErrIntOrDecimalExpressionExpected            errors.Code = "ErrIntOrDecimalExpressionExpected"
	ErrIntOrDecimalOrTimestampExpressionExpected errors.Code = "ErrIntOrDecimalOrTimestampExpressionExpected"
	ErrStringExpressionExpected                  errors.Code = "ErrStringExpressionExpected"
	ErrSetExpressionExpected                     errors.Code = "ErrSetExpressionExpected"
	ErrSingleRowExpected                         errors.Code = "ErrSingleRowExpected"

	// type related errors

	// decimal
	ErrDecimalScaleExpected errors.Code = "ErrDecimalScaleExpected"

	ErrInvalidCast         errors.Code = "ErrInvalidCast"
	ErrInvalidTypeCoercion errors.Code = "ErrInvalidTypeCoercion"

	ErrLiteralExpected                  errors.Code = "ErrLiteralExpected"
	ErrIntegerLiteral                   errors.Code = "ErrIntegerLiteral"
	ErrStringLiteral                    errors.Code = "ErrStringLiteral"
	ErrBoolLiteral                      errors.Code = "ErrBoolLiteral"
	ErrLiteralNullNotAllowed            errors.Code = "ErrLiteralNullNotAllowed"
	ErrLiteralEmptySetNotAllowed        errors.Code = "ErrLiteralEmptySetNotAllowed"
	ErrLiteralEmptyTupleNotAllowed      errors.Code = "ErrLiteralEmptyTupleNotAllowed"
	ErrSetLiteralMustContainIntOrString errors.Code = "ErrSetLiteralMustContainIntOrString"
	ErrInvalidColumnInFilterExpression  errors.Code = "ErrInvalidColumnInFilterExpression"
	ErrInvalidTypeInFilterExpression    errors.Code = "ErrInvalidTypeInFilterExpression"

	ErrTypeAssignmentIncompatible errors.Code = "ErrTypeAssignmentIncompatible"

	ErrInvalidUngroupedColumnReference         errors.Code = "ErrInvalidUngroupedColumnReference"
	ErrInvalidUngroupedColumnReferenceInHaving errors.Code = "ErrInvalidUngroupedColumnReferenceInHaving"

	ErrInvalidTimeUnit    errors.Code = "ErrInvalidTimeUnit"
	ErrInvalidTimeEpoch   errors.Code = "ErrInvalidTimeEpoch"
	ErrInvalidTimeQuantum errors.Code = "ErrInvalidTimeQuantum"
	ErrInvalidDuration    errors.Code = "ErrInvalidDuration"

	ErrInsertExprTargetCountMismatch   errors.Code = "ErrInsertExprTargetCountMismatch"
	ErrInsertMustHaveIDColumn          errors.Code = "ErrInsertMustHaveIDColumn"
	ErrInsertMustAtLeastOneNonIDColumn errors.Code = "ErrInsertMustAtLeastOneNonIDColumn"

	ErrDatabaseExists errors.Code = "ErrDatabaseExists"

	ErrTableMustHaveIDColumn     errors.Code = "ErrTableMustHaveIDColumn"
	ErrTableIDColumnType         errors.Code = "ErrTableIDColumnType"
	ErrTableIDColumnConstraints  errors.Code = "ErrTableIDColumnConstraints"
	ErrTableIDColumnAlter        errors.Code = "ErrTableIDColumnAlter"
	ErrTableNotFound             errors.Code = "ErrTableNotFound"
	ErrTableExists               errors.Code = "ErrTableExists"
	ErrColumnNotFound            errors.Code = "ErrColumnNotFound"
	ErrTableColumnNotFound       errors.Code = "ErrTableColumnNotFound"
	ErrInvalidKeyPartitionsValue errors.Code = "ErrInvalidKeyPartitionsValue"

	ErrTableOrViewNotFound errors.Code = "ErrTableOrViewNotFound"

	ErrViewExists   errors.Code = "ErrViewExists"
	ErrViewNotFound errors.Code = "ErrViewNotFound"

	ErrBadColumnConstraint         errors.Code = "ErrBadColumnConstraint"
	ErrConflictingColumnConstraint errors.Code = "ErrConflictingColumnConstraint"

	// expected errors
	ErrExpectedColumnReference         errors.Code = "ErrExpectedColumnReference"
	ErrExpectedSortExpressionReference errors.Code = "ErrExpectedSortExpressionReference"
	ErrExpectedSortableExpression      errors.Code = "ErrExpectedSortableExpression"

	// call errors
	ErrCallUnknownFunction                  errors.Code = "ErrCallUnknownFunction"
	ErrCallParameterCountMismatch           errors.Code = "ErrCallParameterCountMismatch"
	ErrIdColumnNotValidForAggregateFunction errors.Code = "ErrIdColumnNotValidForAggregateFunction"
	ErrParameterTypeMistmatch               errors.Code = "ErrParameterTypeMistmatch"
	ErrCallParameterValueInvalid            errors.Code = "ErrCallParameterValueInvalid"

	// insert errors

	ErrInsertValueOutOfRange errors.Code = "ErrInsertValueOutOfRange"

	// bulk insert errors

	ErrReadingDatasource       errors.Code = "ErrReadingDatasource"
	ErrMappingFromDatasource   errors.Code = "ErrMappingFromDatasource"
	ErrFormatSpecifierExpected errors.Code = "ErrFormatSpecifierExpected"
	ErrInvalidFormatSpecifier  errors.Code = "ErrInvalidFormatSpecifier"
	ErrInputSpecifierExpected  errors.Code = "ErrInputSpecifierExpected"
	ErrInvalidInputSpecifier   errors.Code = "ErrInvalidInputSpecifier"
	ErrInvalidBatchSize        errors.Code = "ErrInvalidBatchSize"
	ErrTypeConversionOnMap     errors.Code = "ErrTypeConversionOnMap"
	ErrParsingJSON             errors.Code = "ErrParsingJSON"
	ErrEvaluatingJSONPathExpr  errors.Code = "ErrEvaluatingJSONPathExpr"

	// optimizer errors
	ErrAggregateNotAllowedInGroupBy errors.Code = "ErrIdPercentileNotAllowedInGroupBy"

	// function evaluation
	ErrValueOutOfRange          errors.Code = "ErrValueOutOfRange"
	ErrStringLengthMismatch     errors.Code = "ErrStringLengthMismatch"
	ErrUnexpectedTypeConversion errors.Code = "ErrUnexpectedTypeConversion"
)

func NewErrDuplicateColumn(line int, col int, column string) error {
	return errors.New(
		ErrDuplicateColumn,
		fmt.Sprintf("[%d:%d] duplicate column '%s'", line, col, column),
	)
}

func NewErrUnknownType(line int, col int, typ string) error {
	return errors.New(
		ErrUnknownType,
		fmt.Sprintf("[%d:%d] unknown type '%s'", line, col, typ),
	)
}

func NewErrUnknownIdentifier(line int, col int, ident string) error {
	return errors.New(
		ErrUnknownIdentifier,
		fmt.Sprintf("[%d:%d] unknown identifier '%s'", line, col, ident),
	)
}

func NewErrInternal(msg string) error {
	preamble := "internal error"
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		preamble = fmt.Sprintf("internal error (%s:%d)", filename, line)
	}
	errorMessage := fmt.Sprintf("%s %s", preamble, msg)
	return errors.New(
		ErrInternal,
		errorMessage,
	)
}

func NewErrInternalf(format string, a ...interface{}) error {
	preamble := "internal error"
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		preamble = fmt.Sprintf("internal error (%s:%d)", filename, line)
	}
	errorMessage := fmt.Sprintf(format, a...)
	errorMessage = fmt.Sprintf("%s %s", preamble, errorMessage)
	return errors.New(
		ErrInternal,
		errorMessage,
	)
}

func NewErrUnsupported(line, col int, is bool, thing string) error {
	msg := fmt.Sprintf("[%d:%d] %s are not supported", line, col, thing)
	if is {
		msg = fmt.Sprintf("[%d:%d] %s is not supported", line, col, thing)
	}
	return errors.New(
		ErrUnknownIdentifier,
		msg,
	)
}

func NewErrCacheKeyNotFound(key uint64) error {
	return errors.New(
		ErrCacheKeyNotFound,
		fmt.Sprintf("key '%d' not found", key),
	)
}

func NewErrTypeAssignmentIncompatible(line, col int, type1, type2 string) error {
	return errors.New(
		ErrTypeAssignmentIncompatible,
		fmt.Sprintf("[%d:%d] an expression of type '%s' cannot be assigned to type '%s'", line, col, type1, type2),
	)
}

func NewErrInvalidUngroupedColumnReference(line, col int, column string) error {
	return errors.New(
		ErrInvalidUngroupedColumnReference,
		fmt.Sprintf("[%d:%d] column '%s' invalid in select list because it is not aggregated or grouped", line, col, column),
	)
}

func NewErrInvalidUngroupedColumnReferenceInHaving(line, col int, column string) error {
	return errors.New(
		ErrInvalidUngroupedColumnReferenceInHaving,
		fmt.Sprintf("[%d:%d] column '%s' invalid in the having clause because it is not contained in an aggregate or the GROUP BY clause", line, col, column),
	)
}

func NewErrInvalidCast(line, col int, from, to string) error {
	return errors.New(
		ErrInvalidCast,
		fmt.Sprintf("[%d:%d] '%s' cannot be cast to '%s'", line, col, from, to),
	)
}

func NewErrInvalidTypeCoercion(line, col int, from, to string) error {
	return errors.New(
		ErrInvalidTypeCoercion,
		fmt.Sprintf("[%d:%d] unable to convert '%s' to type '%s'", line, col, from, to),
	)
}

func NewErrLiteralExpected(line, col int) error {
	return errors.New(
		ErrLiteralExpected,
		fmt.Sprintf("[%d:%d] literal expression expected", line, col),
	)
}

func NewErrIntegerLiteral(line, col int) error {
	return errors.New(
		ErrIntegerLiteral,
		fmt.Sprintf("[%d:%d] integer literal expected", line, col),
	)
}

func NewErrStringLiteral(line, col int) error {
	return errors.New(
		ErrStringLiteral,
		fmt.Sprintf("[%d:%d] string literal expected", line, col),
	)
}

func NewErrBoolLiteral(line, col int) error {
	return errors.New(
		ErrBoolLiteral,
		fmt.Sprintf("[%d:%d] bool literal expected", line, col),
	)
}

func NewErrLiteralEmptySetNotAllowed(line, col int) error {
	return errors.New(
		ErrLiteralEmptySetNotAllowed,
		fmt.Sprintf("[%d:%d] set literal must contain at least one member", line, col),
	)
}

func NewErrSetLiteralMustContainIntOrString(line, col int) error {
	return errors.New(
		ErrSetLiteralMustContainIntOrString,
		fmt.Sprintf("[%d:%d] set literal must contain ints or strings", line, col),
	)
}

func NewErrLiteralNullNotAllowed(line, col int) error {
	return errors.New(
		ErrLiteralNullNotAllowed,
		fmt.Sprintf("[%d:%d] null literal not allowed", line, col),
	)
}

func NewErrInvalidColumnInFilterExpression(line, col int, column string, op string) error {
	return errors.New(
		ErrInvalidColumnInFilterExpression,
		fmt.Sprintf("[%d:%d] '%s' column cannot be used in a %s filter expression", line, col, column, op),
	)
}

func NewErrInvalidTypeInFilterExpression(line, col int, typeName string, op string) error {
	return errors.New(
		ErrInvalidTypeInFilterExpression,
		fmt.Sprintf("[%d:%d] unsupported type '%s' for %s filter expression", line, col, typeName, op),
	)
}

func NewErrLiteralEmptyTupleNotAllowed(line, col int) error {
	return errors.New(
		ErrLiteralEmptyTupleNotAllowed,
		fmt.Sprintf("[%d:%d] tuple literal must contain at least one member", line, col),
	)
}

func NewErrTypeIncompatibleWithBitwiseOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithBitwiseOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithLogicalOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithLogicalOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithEqualityOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithEqualityOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithComparisonOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithComparisonOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithArithmeticOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithArithmeticOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithConcatOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithConcatOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithLikeOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithLikeOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeIncompatibleWithBetweenOperator(line, col int, operator, type1 string) error {
	return errors.New(
		ErrTypeIncompatibleWithBetweenOperator,
		fmt.Sprintf("[%d:%d] operator '%s' incompatible with type '%s'", line, col, operator, type1),
	)
}

func NewErrTypeCannotBeUsedAsRangeSubscript(line, col int, type1 string) error {
	return errors.New(
		ErrTypeCannotBeUsedAsRangeSubscript,
		fmt.Sprintf("[%d:%d] type '%s' cannot be used as a range subscript", line, col, type1),
	)
}

func NewErrIncompatibleTypesForRangeSubscripts(line, col int, type1 string, type2 string) error {
	return errors.New(
		ErrIncompatibleTypesForRangeSubscripts,
		fmt.Sprintf("[%d:%d] incompatible types '%s' and '%s' useds as range subscripts", line, col, type1, type2),
	)
}

func NewErrTypesAreNotEquatable(line, col int, type1, type2 string) error {
	return errors.New(
		ErrTypesAreNotEquatable,
		fmt.Sprintf("[%d:%d] types '%s' and '%s' are not equatable", line, col, type1, type2),
	)
}

func NewErrTypeMismatch(line, col int, type1, type2 string) error {
	return errors.New(
		ErrTypeMismatch,
		fmt.Sprintf("[%d:%d] types '%s' and '%s' do not match", line, col, type1, type2),
	)
}

func NewErrExpressionListExpected(line, col int) error {
	return errors.New(
		ErrExpressionListExpected,
		fmt.Sprintf("[%d:%d] expression list expected", line, col),
	)
}

func NewErrBooleanExpressionExpected(line, col int) error {
	return errors.New(
		ErrBooleanExpressionExpected,
		fmt.Sprintf("[%d:%d] boolean expression expected", line, col),
	)
}

func NewErrIntExpressionExpected(line, col int) error {
	return errors.New(
		ErrIntExpressionExpected,
		fmt.Sprintf("[%d:%d] integer expression expected", line, col),
	)
}

func NewErrIntOrDecimalExpressionExpected(line, col int) error {
	return errors.New(
		ErrIntOrDecimalExpressionExpected,
		fmt.Sprintf("[%d:%d] integer or decimal expression expected", line, col),
	)
}

func NewErrIntOrDecimalOrTimestampExpressionExpected(line, col int) error {
	return errors.New(
		ErrIntOrDecimalOrTimestampExpressionExpected,
		fmt.Sprintf("[%d:%d] integer, decimal or timestamp expression expected", line, col),
	)
}

func NewErrStringExpressionExpected(line, col int) error {
	return errors.New(
		ErrStringExpressionExpected,
		fmt.Sprintf("[%d:%d] string expression expected", line, col),
	)
}

func NewErrSetExpressionExpected(line, col int) error {
	return errors.New(
		ErrSetExpressionExpected,
		fmt.Sprintf("[%d:%d] set expression expected", line, col),
	)
}

func NewErrSingleRowExpected(line, col int) error {
	return errors.New(
		ErrSingleRowExpected,
		fmt.Sprintf("[%d:%d] single row expected", line, col),
	)
}

// type errors

// decimal related

func NewErrDecimalScaleExpected(line, col int) error {
	return errors.New(
		ErrDecimalScaleExpected,
		fmt.Sprintf("[%d:%d] decimal scale expected", line, col),
	)
}

func NewErrInvalidTimeUnit(line, col int, unit string) error {
	return errors.New(
		ErrInvalidTimeUnit,
		fmt.Sprintf("[%d:%d] '%s' is not a valid time unit", line, col, unit),
	)
}

func NewErrInvalidTimeEpoch(line, col int, epoch string) error {
	return errors.New(
		ErrInvalidTimeEpoch,
		fmt.Sprintf("[%d:%d] '%s' is not a valid epoch", line, col, epoch),
	)
}

func NewErrInvalidTimeQuantum(line, col int, quantum string) error {
	return errors.New(
		ErrInvalidTimeQuantum,
		fmt.Sprintf("[%d:%d] '%s' is not a valid time quantum", line, col, quantum),
	)
}

func NewErrInvalidDuration(line, col int, duration string) error {
	return errors.New(
		ErrInvalidDuration,
		fmt.Sprintf("[%d:%d] '%s' is not a valid time duration", line, col, duration),
	)
}

func NewErrInsertExprTargetCountMismatch(line int, col int) error {
	return errors.New(
		ErrInsertExprTargetCountMismatch,
		fmt.Sprintf("[%d:%d] mismatch in the count of expressions and target columns", line, col),
	)
}

func NewErrInsertMustHaveIDColumn(line int, col int) error {
	return errors.New(
		ErrInsertMustHaveIDColumn,
		fmt.Sprintf("[%d:%d] insert column list must have '_id' column specified", line, col),
	)
}

func NewErrInsertMustAtLeastOneNonIDColumn(line int, col int) error {
	return errors.New(
		ErrInsertMustAtLeastOneNonIDColumn,
		fmt.Sprintf("[%d:%d] insert column list must have at least one non '_id' column specified", line, col),
	)
}

func NewErrDatabaseExists(line, col int, databaseName string) error {
	return errors.New(
		ErrDatabaseExists,
		fmt.Sprintf("[%d:%d] database '%s' already exists", line, col, databaseName),
	)
}

func NewErrTableMustHaveIDColumn(line, col int) error {
	return errors.New(
		ErrTableMustHaveIDColumn,
		fmt.Sprintf("[%d:%d] _id column must be specified", line, col),
	)
}

func NewErrTableIDColumnType(line, col int) error {
	return errors.New(
		ErrTableIDColumnType,
		fmt.Sprintf("[%d:%d] _id column must be specified with type ID or STRING", line, col),
	)
}

func NewErrTableIDColumnConstraints(line, col int) error {
	return errors.New(
		ErrTableIDColumnConstraints,
		fmt.Sprintf("[%d:%d] _id column must be specified with no constraints", line, col),
	)
}

func NewErrTableIDColumnAlter(line, col int) error {
	return errors.New(
		ErrTableIDColumnAlter,
		fmt.Sprintf("[%d:%d] _id column cannot be added to an existing table", line, col),
	)
}

func NewErrTableNotFound(line, col int, tableName string) error {
	return errors.New(
		ErrTableNotFound,
		fmt.Sprintf("[%d:%d] table '%s' not found", line, col, tableName),
	)
}

func NewErrTableOrViewNotFound(line, col int, tableName string) error {
	return errors.New(
		ErrTableOrViewNotFound,
		fmt.Sprintf("[%d:%d] table or view '%s' not found", line, col, tableName),
	)
}

func NewErrTableExists(line, col int, tableName string) error {
	return errors.New(
		ErrTableExists,
		fmt.Sprintf("[%d:%d] table '%s' already exists", line, col, tableName),
	)
}

func NewErrColumnNotFound(line, col int, columnName string) error {
	return errors.New(
		ErrColumnNotFound,
		fmt.Sprintf("[%d:%d] column '%s' not found", line, col, columnName),
	)
}

func NewErrTableColumnNotFound(line, col int, tableName string, columnName string) error {
	return errors.New(
		ErrTableColumnNotFound,
		fmt.Sprintf("[%d:%d] column '%s' not found in table '%s'", line, col, columnName, tableName),
	)
}

func NewErrInvalidKeyPartitionsValue(line, col int, keypartitions int64) error {
	return errors.New(
		ErrInvalidKeyPartitionsValue,
		fmt.Sprintf("[%d:%d] invalid value '%d' for key partitions (should be a number between 1-10000)", line, col, keypartitions),
	)
}

func NewErrViewNotFound(line, col int, viewName string) error {
	return errors.New(
		ErrViewNotFound,
		fmt.Sprintf("[%d:%d] view '%s' not found", line, col, viewName),
	)
}

func NewErrViewExists(line, col int, viewName string) error {
	return errors.New(
		ErrViewExists,
		fmt.Sprintf("[%d:%d] view '%s' already exists", line, col, viewName),
	)
}

func NewErrBadColumnConstraint(line, col int, constraint, columnType string) error {
	return errors.New(
		ErrBadColumnConstraint,
		fmt.Sprintf("[%d:%d] '%s' constraint cannot be applied to a column of type '%s'", line, col, constraint, columnType),
	)
}

func NewErrConflictingColumnConstraint(line, col int, token1, token2 parser.Token) error {
	return errors.New(
		ErrConflictingColumnConstraint,
		fmt.Sprintf("[%d:%d] '%s' constraint conflicts with '%s'", line, col, token1, token2),
	)
}

// expected

func NewErrExpectedColumnReference(line, col int) error {
	return errors.New(
		ErrExpectedColumnReference,
		fmt.Sprintf("[%d:%d] column reference expected", line, col),
	)
}

func NewErrExpectedSortExpressionReference(line, col int) error {
	return errors.New(
		ErrExpectedSortExpressionReference,
		fmt.Sprintf("[%d:%d] column reference, alias reference or column position expected", line, col),
	)
}

func NewErrExpectedSortableExpression(line, col int, typeName string) error {
	return errors.New(
		ErrExpectedSortExpressionReference,
		fmt.Sprintf("[%d:%d] unable to sort a column of type '%s'", line, col, typeName),
	)
}

// calls

func NewErrCallParameterCountMismatch(line, col int, functionName string, formalCount, actualCount int) error {
	return errors.New(
		ErrCallParameterCountMismatch,
		fmt.Sprintf("[%d:%d] '%s': count of formal parameters (%d) does not match count of actual parameters (%d)", line, col, functionName, formalCount, actualCount),
	)
}

func NewErrCallUnknownFunction(line, col int, functionName string) error {
	return errors.New(
		ErrCallUnknownFunction,
		fmt.Sprintf("[%d:%d] unknown function '%s'", line, col, functionName),
	)
}

func NewErrIdColumnNotValidForAggregateFunction(line, col int, functionName string) error {
	return errors.New(
		ErrIdColumnNotValidForAggregateFunction,
		fmt.Sprintf("[%d:%d] _id column cannot be used in aggregate function '%s'", line, col, functionName),
	)
}

func NewErrParameterTypeMistmatch(line, col int, type1, type2 string) error {
	return errors.New(
		ErrParameterTypeMistmatch,
		fmt.Sprintf("[%d:%d] an expression of type '%s' cannot be passed to a parameter of type '%s'", line, col, type1, type2),
	)
}

func NewErrCallParameterValueInvalid(line, col int, badParameterValue string, parameterName string) error {
	return errors.New(
		ErrCallParameterValueInvalid,
		fmt.Sprintf("[%d:%d] invalid value '%s' for parameter '%s'", line, col, badParameterValue, parameterName),
	)
}

// insert

func NewErrInsertValueOutOfRange(line, col int, columnName string, rowNumber int, badValue interface{}) error {
	return errors.New(
		ErrInsertValueOutOfRange,
		fmt.Sprintf("[%d:%d] inserting value into column '%s', row %d, value '%v' out of range", line, col, columnName, rowNumber, badValue),
	)
}

// bulk insert

func NewErrReadingDatasource(line, col int, dataSource string, errorText string) error {
	return errors.New(
		ErrReadingDatasource,
		fmt.Sprintf("[%d:%d] unable to read datasource '%s': %s", line, col, dataSource, errorText),
	)
}

func NewErrMappingFromDatasource(line, col int, dataSource string, errorText string) error {
	return errors.New(
		ErrMappingFromDatasource,
		fmt.Sprintf("[%d:%d] unable to map from datasource '%s': %s", line, col, dataSource, errorText),
	)
}

func NewErrFormatSpecifierExpected(line, col int) error {
	return errors.New(
		ErrFormatSpecifierExpected,
		fmt.Sprintf("[%d:%d] format specifier expected", line, col),
	)
}

func NewErrInvalidFormatSpecifier(line, col int, specifier string) error {
	return errors.New(
		ErrInvalidFormatSpecifier,
		fmt.Sprintf("[%d:%d] invalid format specifier '%s'", line, col, specifier),
	)
}

func NewErrInputSpecifierExpected(line, col int) error {
	return errors.New(
		ErrInputSpecifierExpected,
		fmt.Sprintf("[%d:%d] input specifier expected", line, col),
	)
}

func NewErrInvalidInputSpecifier(line, col int, specifier string) error {
	return errors.New(
		ErrInvalidFormatSpecifier,
		fmt.Sprintf("[%d:%d] invalid input specifier '%s'", line, col, specifier),
	)
}

func NewErrInvalidBatchSize(line, col int, batchSize int) error {
	return errors.New(
		ErrInvalidBatchSize,
		fmt.Sprintf("[%d:%d] invalid batch size '%d'", line, col, batchSize),
	)
}

func NewErrTypeConversionOnMap(line, col int, value interface{}, typeName string) error {
	return errors.New(
		ErrTypeConversionOnMap,
		fmt.Sprintf("[%d:%d] value '%v' cannot be converted to type '%s'", line, col, value, typeName),
	)
}

func NewErrParsingJSON(line, col int, jsonString string, errorText string) error {
	return errors.New(
		ErrParsingJSON,
		fmt.Sprintf("[%d:%d] unable to parse JSON '%s': %s", line, col, jsonString, errorText),
	)
}

func NewErrEvaluatingJSONPathExpr(line, col int, exprText string, jsonString string, errorText string) error {
	return errors.New(
		ErrEvaluatingJSONPathExpr,
		fmt.Sprintf("[%d:%d] unable to evaluate JSONPath expression '%s' in '%s': %s", line, col, exprText, jsonString, errorText),
	)
}

// optimizer

func NewErrAggregateNotAllowedInGroupBy(line, col int, aggName string) error {
	return errors.New(
		ErrAggregateNotAllowedInGroupBy,
		fmt.Sprintf("[%d:%d] aggregate '%s' not allowed in GROUP BY", line, col, aggName),
	)
}

// function evaluation
func NewErrValueOutOfRange(line, col int, val interface{}) error {
	return errors.New(
		ErrValueOutOfRange,
		fmt.Sprintf("[%d:%d] value '%v' out of range", line, col, val),
	)
}

func NewErrStringLengthMismatch(line, col, len int, val interface{}) error {
	return errors.New(
		ErrStringLengthMismatch,
		fmt.Sprintf("[%d:%d] value '%v' should be of the length %d", line, col, val, len),
	)
}

func NewErrUnexpectedTypeConversion(line, col int, val interface{}) error {
	return errors.New(
		ErrUnexpectedTypeConversion,
		NewErrInternalf("unexpected type conversion %T", val).Error(),
	)
}
