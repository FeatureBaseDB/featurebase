package parser

import (
	"fmt"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
)

func IsValidTypeName(typeName string) bool {
	switch strings.ToLower(typeName) {
	case dax.BaseTypeBool,
		dax.BaseTypeDecimal,
		dax.BaseTypeID,
		dax.BaseTypeIDSet,
		dax.BaseTypeInt,
		dax.BaseTypeString,
		dax.BaseTypeStringSet,
		dax.BaseTypeTimestamp:
		return true
	default:
		return false
	}
}

// ExprDataType is the interface for all language layer types
type ExprDataType interface {
	exprDataType()
	// the base type name e.g. int or decimal
	BaseTypeName() string
	// additional type information - intended to be used outside the language
	// layer (marshalled over json, or otherwise serialized so that consumers
	// have access to complete type information)
	// TypeInfo is not used inside the language layer itself, as the concrete
	// types (e.g. DataTypeString) are used
	TypeInfo() map[string]interface{}
	// the full type specification as a string - intended to be human readable
	TypeDescription() string
}

func (*DataTypeVoid) exprDataType()             {}
func (*DataTypeRange) exprDataType()            {}
func (*DataTypeTuple) exprDataType()            {}
func (*DataTypeSubtable) exprDataType()         {}
func (*DataTypeBool) exprDataType()             {}
func (*DataTypeDecimal) exprDataType()          {}
func (*DataTypeID) exprDataType()               {}
func (*DataTypeIDSet) exprDataType()            {}
func (*DataTypeIDSetQuantum) exprDataType()     {}
func (*DataTypeInt) exprDataType()              {}
func (*DataTypeString) exprDataType()           {}
func (*DataTypeStringSet) exprDataType()        {}
func (*DataTypeStringSetQuantum) exprDataType() {}
func (*DataTypeTimestamp) exprDataType()        {}

type DataTypeVoid struct {
}

func NewDataTypeVoid() *DataTypeVoid {
	return &DataTypeVoid{}
}

func (*DataTypeVoid) BaseTypeName() string {
	return "void"
}

func (dt *DataTypeVoid) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeVoid) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeRange struct {
	SubscriptType ExprDataType
}

func NewDataTypeRange(subscriptType ExprDataType) *DataTypeRange {
	return &DataTypeRange{
		SubscriptType: subscriptType,
	}
}

func (dt *DataTypeRange) BaseTypeName() string {
	return "range"
}

func (dt *DataTypeRange) TypeDescription() string {
	return fmt.Sprintf("range(%s)", dt.SubscriptType.TypeDescription())
}

func (*DataTypeRange) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeTuple struct {
	Members []ExprDataType
}

func NewDataTypeTuple(members []ExprDataType) *DataTypeTuple {
	return &DataTypeTuple{
		Members: members,
	}
}

func (dt *DataTypeTuple) BaseTypeName() string {
	return "tuple"
}

func (dt *DataTypeTuple) TypeDescription() string {
	ms := ""
	for idx, m := range dt.Members {
		ms = ms + m.TypeDescription()
		if idx+1 < len(dt.Members) {
			ms = ms + ", "
		}
	}
	return fmt.Sprintf("tuple(%s)", ms)
}

func (*DataTypeTuple) TypeInfo() map[string]interface{} {
	return nil
}

type SubtableColumn struct {
	Name     string
	DataType ExprDataType
}

type DataTypeSubtable struct {
	Columns []*SubtableColumn
}

func NewDataTypeSubtable(columns []*SubtableColumn) *DataTypeSubtable {
	return &DataTypeSubtable{
		Columns: columns,
	}
}

func (dt *DataTypeSubtable) BaseTypeName() string {
	return "subtable"
}

func (dt *DataTypeSubtable) TypeDescription() string {
	ms := ""
	for idx, m := range dt.Columns {
		ms = ms + m.DataType.TypeDescription()
		if idx+1 < len(dt.Columns) {
			ms = ms + ", "
		}
	}
	return fmt.Sprintf("subtable(%s)", ms)
}

func (*DataTypeSubtable) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeBool struct {
}

func NewDataTypeBool() *DataTypeBool {
	return &DataTypeBool{}
}

func (*DataTypeBool) BaseTypeName() string {
	return dax.BaseTypeBool
}

func (dt *DataTypeBool) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeBool) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeDecimal struct {
	Scale int64
}

func NewDataTypeDecimal(scale int64) *DataTypeDecimal {
	return &DataTypeDecimal{
		Scale: scale,
	}
}

func (d *DataTypeDecimal) BaseTypeName() string {
	return dax.BaseTypeDecimal
}

func (d *DataTypeDecimal) TypeDescription() string {
	return fmt.Sprintf("%s(%d)", dax.BaseTypeDecimal, d.Scale)
}

func (d *DataTypeDecimal) TypeInfo() map[string]interface{} {
	return map[string]interface{}{
		"scale": d.Scale,
	}
}

type DataTypeID struct {
}

func NewDataTypeID() *DataTypeID {
	return &DataTypeID{}
}

func (*DataTypeID) BaseTypeName() string {
	return dax.BaseTypeID
}

func (dt *DataTypeID) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeID) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeIDSet struct {
}

func NewDataTypeIDSet() *DataTypeIDSet {
	return &DataTypeIDSet{}
}

func (*DataTypeIDSet) BaseTypeName() string {
	return dax.BaseTypeIDSet
}

func (dt *DataTypeIDSet) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeIDSet) TypeInfo() map[string]interface{} {
	return nil
}

// TODO (pok) should time quantum be it's own type and not a constraint?
type DataTypeIDSetQuantum struct {
}

func NewDataTypeIDSetQuantum() *DataTypeIDSetQuantum {
	return &DataTypeIDSetQuantum{}
}

func (*DataTypeIDSetQuantum) BaseTypeName() string {
	return dax.BaseTypeIDSet
}

func (dt *DataTypeIDSetQuantum) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeIDSetQuantum) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeInt struct {
}

func NewDataTypeInt() *DataTypeInt {
	return &DataTypeInt{}
}

func (*DataTypeInt) BaseTypeName() string {
	return dax.BaseTypeInt
}

func (dt *DataTypeInt) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeInt) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeString struct {
}

func NewDataTypeString() *DataTypeString {
	return &DataTypeString{}
}

func (*DataTypeString) BaseTypeName() string {
	return dax.BaseTypeString
}

func (dt *DataTypeString) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeString) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeStringSet struct {
}

func NewDataTypeStringSet() *DataTypeStringSet {
	return &DataTypeStringSet{}
}

func (*DataTypeStringSet) BaseTypeName() string {
	return dax.BaseTypeStringSet
}

func (dt *DataTypeStringSet) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeStringSet) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeStringSetQuantum struct {
}

func NewDataTypeStringSetQuantum() *DataTypeStringSetQuantum {
	return &DataTypeStringSetQuantum{}
}

func (*DataTypeStringSetQuantum) BaseTypeName() string {
	return dax.BaseTypeStringSet
}

func (dt *DataTypeStringSetQuantum) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeStringSetQuantum) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeTimestamp struct {
}

func NewDataTypeTimestamp() *DataTypeTimestamp {
	return &DataTypeTimestamp{}
}

func (*DataTypeTimestamp) BaseTypeName() string {
	return dax.BaseTypeTimestamp
}

func (dt *DataTypeTimestamp) TypeDescription() string {
	return dt.BaseTypeName()
}

func (*DataTypeTimestamp) TypeInfo() map[string]interface{} {
	return nil
}

func NumDecimalPlaces(v string) int {
	i := strings.IndexByte(v, '.')
	if i > -1 {
		return len(v) - i - 1
	}
	return 0
}
