package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/molecula/featurebase/v3/pql"
)

const (
	FieldTypeBool             = "BOOL"
	FieldTypeDecimal          = "DECIMAL"
	FieldTypeID               = "ID"
	FieldTypeIDSet            = "IDSET"
	FieldTypeIDSetQuantum     = "IDSETQ"
	FieldTypeInt              = "INT"
	FieldTypeString           = "STRING"
	FieldTypeStringSet        = "STRINGSET"
	FieldTypeStringSetQuantum = "STRINGSETQ"
	FieldTypeTimestamp        = "TIMESTAMP"
)

func IsValidTypeName(typeName string) bool {
	switch strings.ToUpper(typeName) {
	case FieldTypeBool,
		FieldTypeDecimal,
		FieldTypeID,
		FieldTypeIDSet,
		FieldTypeInt,
		FieldTypeString,
		FieldTypeStringSet,
		FieldTypeTimestamp:
		return true
	default:
		return false
	}
}

type ExprDataType interface {
	exprDataType()
	TypeName() string
	TypeDescription() string
	TypeInfo() map[string]interface{}
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

func (*DataTypeVoid) TypeName() string {
	return "VOID"
}

func (dt *DataTypeVoid) TypeDescription() string {
	return dt.TypeName()
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

func (dt *DataTypeRange) TypeName() string {
	return fmt.Sprintf("RANGE(%s)", dt.SubscriptType.TypeName())
}

func (dt *DataTypeRange) TypeDescription() string {
	return dt.TypeName()
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

func (dt *DataTypeTuple) TypeName() string {
	ms := ""
	for idx, m := range dt.Members {
		ms = ms + m.TypeName()
		if idx+1 < len(dt.Members) {
			ms = ms + ", "
		}
	}
	return fmt.Sprintf("TUPLE(%s)", ms)
}

func (dt *DataTypeTuple) TypeDescription() string {
	return dt.TypeName()
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

func (dt *DataTypeSubtable) TypeName() string {
	ms := ""
	for idx, m := range dt.Columns {
		ms = ms + m.DataType.TypeName()
		if idx+1 < len(dt.Columns) {
			ms = ms + ", "
		}
	}
	return fmt.Sprintf("SUBTABLE(%s)", ms)
}

func (dt *DataTypeSubtable) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeSubtable) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeBool struct {
}

func NewDataTypeBool() *DataTypeBool {
	return &DataTypeBool{}
}

func (*DataTypeBool) TypeName() string {
	return FieldTypeBool
}

func (dt *DataTypeBool) TypeDescription() string {
	return dt.TypeName()
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

func (d *DataTypeDecimal) TypeName() string {
	return FieldTypeDecimal
}

func (d *DataTypeDecimal) TypeDescription() string {
	return fmt.Sprintf("%s(%d)", FieldTypeDecimal, d.Scale)
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

func (*DataTypeID) TypeName() string {
	return FieldTypeID
}

func (dt *DataTypeID) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeID) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeIDSet struct {
}

func NewDataTypeIDSet() *DataTypeIDSet {
	return &DataTypeIDSet{}
}

func (*DataTypeIDSet) TypeName() string {
	return FieldTypeIDSet
}

func (dt *DataTypeIDSet) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeIDSet) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeIDSetQuantum struct {
}

func NewDataTypeIDSetQuantum() *DataTypeIDSetQuantum {
	return &DataTypeIDSetQuantum{}
}

func (*DataTypeIDSetQuantum) TypeName() string {
	return FieldTypeIDSetQuantum
}

func (dt *DataTypeIDSetQuantum) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeIDSetQuantum) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeInt struct {
}

func NewDataTypeInt() *DataTypeInt {
	return &DataTypeInt{}
}

func (*DataTypeInt) TypeName() string {
	return FieldTypeInt
}

func (dt *DataTypeInt) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeInt) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeString struct {
}

func NewDataTypeString() *DataTypeString {
	return &DataTypeString{}
}

func (*DataTypeString) TypeName() string {
	return FieldTypeString
}

func (dt *DataTypeString) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeString) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeStringSet struct {
}

func NewDataTypeStringSet() *DataTypeStringSet {
	return &DataTypeStringSet{}
}

func (*DataTypeStringSet) TypeName() string {
	return FieldTypeStringSet
}

func (dt *DataTypeStringSet) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeStringSet) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeStringSetQuantum struct {
}

func NewDataTypeStringSetQuantum() *DataTypeStringSetQuantum {
	return &DataTypeStringSetQuantum{}
}

func (*DataTypeStringSetQuantum) TypeName() string {
	return FieldTypeStringSetQuantum
}

func (dt *DataTypeStringSetQuantum) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeStringSetQuantum) TypeInfo() map[string]interface{} {
	return nil
}

type DataTypeTimestamp struct {
}

func NewDataTypeTimestamp() *DataTypeTimestamp {
	return &DataTypeTimestamp{}
}

func (*DataTypeTimestamp) TypeName() string {
	return FieldTypeTimestamp
}

func (dt *DataTypeTimestamp) TypeDescription() string {
	return dt.TypeName()
}

func (*DataTypeTimestamp) TypeInfo() map[string]interface{} {
	return nil
}

func StringToDecimal(v string) (pql.Decimal, error) {
	fvalue, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return pql.NewDecimal(0, 0), err
	}
	return FloatToDecimal(fvalue), nil
}

func FloatToDecimal(v float64) pql.Decimal {
	scale := NumDecimalPlaces(fmt.Sprintf("%v", v))
	unscaledValue := int64(v * math.Pow(10, float64(scale)))
	return pql.NewDecimal(unscaledValue, int64(scale))
}

func FloatToDecimalWithScale(v float64, s int64) pql.Decimal {
	unscaledValue := int64(v * math.Pow(10, float64(s)))
	return pql.NewDecimal(unscaledValue, int64(s))
}

func NumDecimalPlaces(v string) int {
	i := strings.IndexByte(v, '.')
	if i > -1 {
		return len(v) - i - 1
	}
	return 0
}
