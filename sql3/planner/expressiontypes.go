// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"strconv"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

// takes a *pilosa.FieldInfo and returns a sql data type
func fieldSQLDataType(f *pilosa.FieldInfo) parser.ExprDataType {
	// This is special handling for the primary key (_id) field. The normal
	// handling below was not well suited to this field because there isn't a
	// `pilosa.FieldTypeID` to compare against. One option would have been to
	// add that to field.go, but it seemed risky to add a FieldType which
	// FeatureBase is currently not using. In the future, this logic should use
	// the next generation of FieldTypes in the dax package, which does include
	// a FieldTypeID. Another thing to be updated is the "_id" value itself; in
	// the dax package there is a constant called `PrimaryKeyFieldName` which
	// would be used here instead.
	if f.Name == "_id" {
		switch f.Options.Type {
		case "id":
			return parser.NewDataTypeID()
		case "string":
			return parser.NewDataTypeString()
		default:
			return parser.NewDataTypeVoid()
		}
	}

	switch f.Options.Type {
	case pilosa.FieldTypeInt:
		return parser.NewDataTypeInt()

	case pilosa.FieldTypeMutex:
		if f.Options.Keys {
			return parser.NewDataTypeString()
		} else {
			return parser.NewDataTypeID()
		}

	case pilosa.FieldTypeSet:
		if f.Options.Keys {
			return parser.NewDataTypeStringSet()
		} else {
			return parser.NewDataTypeIDSet()
		}

	case pilosa.FieldTypeBool:
		return parser.NewDataTypeBool()

	case pilosa.FieldTypeDecimal:
		return parser.NewDataTypeDecimal(f.Options.Scale)

	case pilosa.FieldTypeTime, pilosa.FieldTypeTimestamp:
		return parser.NewDataTypeTimestamp()

	default:
		return parser.NewDataTypeVoid()
	}
}

// resolves type names to type representations
func dataTypeFromParserType(typ *parser.Type) (parser.ExprDataType, error) {
	typeName := parser.IdentName(typ.Name)
	switch strings.ToUpper(typeName) {
	case parser.FieldTypeBool:
		return parser.NewDataTypeBool(), nil

	case parser.FieldTypeDecimal:
		scale, err := strconv.Atoi(typ.Scale.Value)
		if err != nil {
			return nil, err
		}
		return parser.NewDataTypeDecimal(int64(scale)), nil

	case parser.FieldTypeID:
		return parser.NewDataTypeID(), nil

	case parser.FieldTypeIDSet:
		return parser.NewDataTypeIDSet(), nil

	case parser.FieldTypeInt:
		return parser.NewDataTypeInt(), nil

	case parser.FieldTypeString:
		return parser.NewDataTypeString(), nil

	case parser.FieldTypeStringSet:
		return parser.NewDataTypeStringSet(), nil

	case parser.FieldTypeTimestamp:
		return parser.NewDataTypeTimestamp(), nil

	default:
		return nil, sql3.NewErrUnknownType(typ.Name.NamePos.Line, typ.Name.NamePos.Column, typeName)
	}
}

// returns true if type is compatible with logical operators (AND, OR)
func typeIsCompatibleWithLogicalOperator(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt, *parser.DataTypeBool:
		return true
	default:
		return false
	}
}

// returns true if type is compatible with equality operators (=, <>)
func typeIsCompatibleWithEqualityOperator(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt,
		*parser.DataTypeDecimal, *parser.DataTypeBool,
		*parser.DataTypeString, *parser.DataTypeTimestamp,
		*parser.DataTypeIDSet, *parser.DataTypeStringSet:
		return true
	default:
		return false
	}
}

// returns true if type is compatible with comparison operators (<, <=, >, >=)
func typeIsCompatibleWithComparisonOperator(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt, *parser.DataTypeDecimal, *parser.DataTypeTimestamp:
		return true
	default:
		return false
	}
}

// returns true if type is compatible with arithmetic operators (+, -, *, /, %)
func typeIsCompatibleWithArithmeticOperator(testType parser.ExprDataType, op parser.Token) bool {
	switch testType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt:
		return true
	case *parser.DataTypeDecimal:
		return op != parser.REM
	default:
		return false
	}
}

// returns true if type is compatible with concatenation operator (||)
func typeIsCompatibleWithConcatOperator(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeString:
		return true
	default:
		return false
	}
}

// returns true if type is compatible using a range comparison. rhs must be a range type and the lhs must be
// compatible with the range subscript type
func typesAreRangeComparable(testTypeL parser.ExprDataType, testTypeR parser.ExprDataType) (bool, error) {
	switch rhsType := testTypeR.(type) {
	case *parser.DataTypeRange:
		switch rhsType.SubscriptType.(type) {
		case *parser.DataTypeTimestamp:
			switch lhsType := testTypeL.(type) {
			case *parser.DataTypeTimestamp:
				return true, nil

			default:
				return false, sql3.NewErrInternalf("unhandled rhs type '%T' for lhs type '%T'", rhsType, lhsType)

			}
		case *parser.DataTypeInt, *parser.DataTypeID:
			switch lhsType := testTypeL.(type) {
			case *parser.DataTypeID:
				return true, nil
			case *parser.DataTypeInt:
				return true, nil

			default:
				return false, sql3.NewErrInternalf("unhandled rhs type '%T' for lhs type '%T'", rhsType, lhsType)

			}
		}
	default:
		return false, sql3.NewErrInternalf("type '%T' is not a range type", rhsType)
	}
	return false, nil
}

// returns true if type is compatible with the (NOT) LIKE operator
func typeIsCompatibleWithLikeOperator(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeString:
		return true
	default:
		return false
	}
}

// returns true if type is compatible with bitwise operators (&, |, <<, >>)
func typeIsCompatibleWithBitwiseOperator(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt:
		return true
	default:
		return false
	}
}

// returns true if type is assignemt compatible. Assignment compatibility can be either because the
// comparison types are the same or because the source type can be coerced into the the target type
func typesAreAssignmentCompatible(targetType parser.ExprDataType, sourceType parser.ExprDataType) bool {
	//ok to assign null to something
	_, ok := sourceType.(*parser.DataTypeVoid)
	if ok {
		return true
	}

	switch lhs := targetType.(type) {

	case *parser.DataTypeInt:
		switch sourceType.(type) {
		case *parser.DataTypeInt:
			return true
		default:
			return false
		}

	case *parser.DataTypeBool:
		switch sourceType.(type) {
		case *parser.DataTypeBool:
			return true
		default:
			return false
		}

	case *parser.DataTypeID:
		switch sourceType.(type) {
		case *parser.DataTypeInt:
			return true
		case *parser.DataTypeID:
			return true
		default:
			return false
		}
	case *parser.DataTypeStringSet:
		switch sourceType.(type) {
		case *parser.DataTypeStringSet:
			return true
		default:
			return false
		}
	case *parser.DataTypeIDSet:
		switch sourceType.(type) {
		case *parser.DataTypeIDSet:
			return true
		default:
			return false
		}
	case *parser.DataTypeDecimal:
		switch rhs := sourceType.(type) {
		case *parser.DataTypeDecimal:
			//if lhs scale is >= rhs scale, we're good
			return lhs.Scale >= rhs.Scale
		case *parser.DataTypeInt:
			return true
		default:
			return false
		}

	case *parser.DataTypeTimestamp:
		switch sourceType.(type) {
		case *parser.DataTypeTimestamp:
			return true
		case *parser.DataTypeString:
			//could be a string parseable as a date
			return true
		default:
			return false
		}

	case *parser.DataTypeString:
		switch sourceType.(type) {
		case *parser.DataTypeString:
			return true
		default:
			return false
		}

	default:
		return false
	}
}

// returns true if the type can be used as a range subscript
func typeCanBeUsedInRange(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt, *parser.DataTypeTimestamp:
		return true
	default:
		return false
	}
}

// returns true if the types can be considered the same when used as a range bounds
func typesOfRangeBoundsAreTheSame(testTypeL parser.ExprDataType, testTypeR parser.ExprDataType) bool {
	switch testTypeL.(type) {
	case *parser.DataTypeInt:
		switch testTypeR.(type) {
		case *parser.DataTypeInt:
			return true

		case *parser.DataTypeID:
			return true

		default:
			return false
		}

	case *parser.DataTypeID:
		switch testTypeR.(type) {
		case *parser.DataTypeID:
			return true

		case *parser.DataTypeInt:
			return true

		default:
			return false
		}

	case *parser.DataTypeTimestamp:
		switch testTypeR.(type) {
		case *parser.DataTypeTimestamp:
			return true

		default:
			return false
		}

	default:
		return false
	}
}

// returns true if the type is a range type
func typeIsRange(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeRange:
		return true
	default:
		return false
	}
}

// returns true if the type is a set type
func typeIsSet(testType parser.ExprDataType) (bool, parser.ExprDataType) {
	switch testType.(type) {
	case *parser.DataTypeIDSet:
		return true, parser.NewDataTypeID()
	case *parser.DataTypeStringSet:
		return true, parser.NewDataTypeString()
	default:
		return false, nil
	}
}

// returns true if the type is bool
func typeIsBool(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeBool:
		return true
	default:
		return false
	}
}

// returns true if the type is an integer or can be treated as one
func typeIsInteger(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt:
		return true
	default:
		return false
	}
}

// returns true if the type is string
func typeIsString(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeString:
		return true
	default:
		return false
	}
}

// returns true if the type is timestamp
func typeIsTimestamp(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeTimestamp:
		return true
	default:
		return false
	}
}

// returns true if the type is a float
func typeIsFloat(testType parser.ExprDataType) bool {
	switch testType.(type) {
	case *parser.DataTypeDecimal:
		return true
	default:
		return false
	}
}

// returns true if the types can be compared
func typesAreComparable(testTypeL parser.ExprDataType, testTypeR parser.ExprDataType) bool {
	switch testTypeL.(type) {
	case *parser.DataTypeInt:
		switch testTypeR.(type) {
		case *parser.DataTypeInt:
			return true
		case *parser.DataTypeID:
			return true
		case *parser.DataTypeDecimal:
			return true

		}

	case *parser.DataTypeID:
		switch testTypeR.(type) {
		case *parser.DataTypeID:
			return true
		case *parser.DataTypeInt:
			return true
		case *parser.DataTypeDecimal:
			return true

		}

	case *parser.DataTypeDecimal:
		switch testTypeR.(type) {
		case *parser.DataTypeID:
			return true
		case *parser.DataTypeInt:
			return true
		case *parser.DataTypeDecimal:
			return true
		}

	case *parser.DataTypeBool:
		switch testTypeR.(type) {
		case *parser.DataTypeBool:
			return true
		}

	case *parser.DataTypeTimestamp:
		switch testTypeR.(type) {
		case *parser.DataTypeTimestamp:
			return true
		}

	case *parser.DataTypeIDSet:
		switch testTypeR.(type) {
		case *parser.DataTypeIDSet:
			return true

		}

	case *parser.DataTypeString:
		switch testTypeR.(type) {
		case *parser.DataTypeString:
			return true

		}

	case *parser.DataTypeStringSet:
		switch testTypeR.(type) {
		case *parser.DataTypeStringSet:
			return true

		}

	}
	return false
}

// returns the target type given two operand types for an artitmentic operation (or an error)
func typesCoercedForArithmeticOperator(testTypeL parser.ExprDataType, testTypeR parser.ExprDataType, atPos parser.Pos) (parser.ExprDataType, error) {
	switch lhsType := testTypeL.(type) {
	case *parser.DataTypeInt:
		switch rhsType := testTypeR.(type) {
		case *parser.DataTypeInt:
			return rhsType, nil

		case *parser.DataTypeID:
			return lhsType, nil

		case *parser.DataTypeDecimal:
			return rhsType, nil
		}

	case *parser.DataTypeID:
		switch testTypeR.(type) {
		case *parser.DataTypeID:
			return testTypeR, nil

		case *parser.DataTypeInt:
			return testTypeR, nil
		}

	case *parser.DataTypeDecimal:
		switch rhsType := testTypeR.(type) {
		case *parser.DataTypeInt:
			return testTypeL, nil

		case *parser.DataTypeID:
			return testTypeL, nil

		case *parser.DataTypeDecimal:
			if lhsType.Scale > rhsType.Scale {
				return lhsType, nil
			}
			return rhsType, nil
		}

	}
	return nil, sql3.NewErrTypeMismatch(atPos.Line, atPos.Column, testTypeL.TypeName(), testTypeR.TypeName())
}

// returns the target type given two operand types for a bitwise operation (or an error)
func typesCoercedForBitwiseOperator(testTypeL parser.ExprDataType, testTypeR parser.ExprDataType, atPos parser.Pos) (parser.ExprDataType, error) {
	switch testTypeL.(type) {

	case *parser.DataTypeInt:
		switch rhsType := testTypeR.(type) {
		case *parser.DataTypeInt:
			return testTypeL, nil

		case *parser.DataTypeID:
			return testTypeL, nil

		case *parser.DataTypeDecimal:
			return parser.NewDataTypeDecimal(rhsType.Scale), nil

		}

	case *parser.DataTypeID:
		switch rhsType := testTypeR.(type) {
		case *parser.DataTypeID:
			return testTypeR, nil

		case *parser.DataTypeInt:
			return testTypeR, nil

		case *parser.DataTypeDecimal:
			return parser.NewDataTypeDecimal(rhsType.Scale), nil
		}
	}
	return nil, sql3.NewErrTypeMismatch(atPos.Line, atPos.Column, testTypeL.TypeName(), testTypeR.TypeName())
}

// returns the target type given two operand types type coercion operation (or an error)
func typeCoerceType(testTypeL parser.ExprDataType, testTypeR parser.ExprDataType, atPos parser.Pos) (parser.ExprDataType, error) {
	switch testTypeL.(type) {
	case *parser.DataTypeBool:
		switch testTypeR.(type) {
		case *parser.DataTypeBool:
			return testTypeL, nil

		}

	case *parser.DataTypeInt:
		switch testTypeR.(type) {
		case *parser.DataTypeInt:
			return testTypeL, nil
		case *parser.DataTypeID:
			return testTypeL, nil
		case *parser.DataTypeDecimal:
			return testTypeR, nil
		}

	case *parser.DataTypeDecimal:
		switch testTypeR.(type) {
		case *parser.DataTypeDecimal:
			return testTypeL, nil
		case *parser.DataTypeInt:
			return testTypeL, nil
		case *parser.DataTypeID:
			return testTypeL, nil

		}

	case *parser.DataTypeID:
		switch testTypeR.(type) {
		case *parser.DataTypeID:
			return testTypeL, nil
		case *parser.DataTypeInt:
			return testTypeR, nil

		}

	case *parser.DataTypeIDSet:
		switch testTypeR.(type) {
		case *parser.DataTypeIDSet:
			return testTypeL, nil

		}

	case *parser.DataTypeString:
		switch testTypeR.(type) {
		case *parser.DataTypeString:
			return testTypeL, nil

		}

	case *parser.DataTypeStringSet:
		switch testTypeR.(type) {
		case *parser.DataTypeStringSet:
			return testTypeL, nil

		}

	case *parser.DataTypeTimestamp:
		switch testTypeR.(type) {
		case *parser.DataTypeTimestamp:
			return testTypeL, nil

		}

	default:
		return nil, sql3.NewErrInternalf("unhandled lhs type '%t'", testTypeL)
	}
	return nil, sql3.NewErrTypeMismatch(atPos.Line, atPos.Line, testTypeL.TypeName(), testTypeR.TypeName())
}

// returns true if source type can be cast to target type
func typesCanBeCast(sourceType parser.ExprDataType, targetType parser.ExprDataType) bool {
	switch st := sourceType.(type) {
	case *parser.DataTypeInt:
		switch targetType.(type) {
		case *parser.DataTypeInt,
			*parser.DataTypeBool,
			*parser.DataTypeDecimal,
			*parser.DataTypeID,
			*parser.DataTypeString,
			*parser.DataTypeTimestamp:
			return true
		}

	case *parser.DataTypeBool:
		switch targetType.(type) {
		case *parser.DataTypeBool,
			*parser.DataTypeInt,
			*parser.DataTypeString:
			return true
		}

	case *parser.DataTypeDecimal:
		switch tt := targetType.(type) {
		case *parser.DataTypeDecimal:
			return tt.Scale >= st.Scale
		case *parser.DataTypeString:
			return true
		}

	case *parser.DataTypeID:
		switch targetType.(type) {
		case *parser.DataTypeInt,
			*parser.DataTypeBool,
			*parser.DataTypeDecimal,
			*parser.DataTypeID:
			return true
		}

	case *parser.DataTypeIDSet:
		switch targetType.(type) {
		case *parser.DataTypeIDSet, *parser.DataTypeString:
			return true
		}

	case *parser.DataTypeString:
		switch targetType.(type) {
		case *parser.DataTypeInt,
			*parser.DataTypeBool,
			*parser.DataTypeDecimal,
			*parser.DataTypeID,
			*parser.DataTypeString,
			*parser.DataTypeTimestamp:
			return true
		}
	case *parser.DataTypeStringSet:
		switch targetType.(type) {
		case *parser.DataTypeStringSet, *parser.DataTypeString:
			return true
		}

	case *parser.DataTypeTimestamp:
		switch targetType.(type) {
		case *parser.DataTypeInt, *parser.DataTypeTimestamp, *parser.DataTypeString:
			return true
		}

	}
	return false
}
