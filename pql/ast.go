// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pql

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa/ext"
)

// Query represents a PQL query.
type Query struct {
	Calls []*Call

	callStack   []*callStackElem
	conditional []string
}

func (q *Query) startCall(name string) {
	newCall := &Call{Name: name}
	q.callStack = append(q.callStack, &callStackElem{call: newCall})

	if len(q.callStack) == 1 {
		q.Calls = append(q.Calls, newCall)
	} else if prevElem := q.callStack[len(q.callStack)-2]; prevElem.lastField == "" {
		prevElem.call.Children = append(prevElem.call.Children, newCall)
	}
}

// endCall removes the last element from the call stack and returns the call.
func (q *Query) endCall() *Call {
	elem := q.callStack[len(q.callStack)-1]
	q.callStack[len(q.callStack)-1] = nil
	q.callStack = q.callStack[:len(q.callStack)-1]
	return elem.call
}

func (q *Query) lastCallStackElem() *callStackElem {
	if len(q.callStack) == 0 {
		return nil
	}
	return q.callStack[len(q.callStack)-1]
}

func (q *Query) addPosNum(key, value string) {
	q.addField(key)
	q.addNumVal(value)
}

func (q *Query) addPosStr(key, value string) {
	q.addField(key)
	q.addVal(value)
}

func (q *Query) startConditional() {
	q.conditional = make([]string, 0)
	elem := q.lastCallStackElem()
	if elem.call.Args == nil {
		elem.call.Args = make(map[string]interface{})
	}
}

func (q *Query) condAdd(val string) {
	q.conditional = append(q.conditional, val)
}

func (q *Query) endConditional() {
	// do stuff
	if len(q.conditional) != 5 {
		panic(fmt.Sprintf("conditional of wrong length: %#v", q.conditional))
	}
	low, _ := strconv.ParseInt(q.conditional[0], 10, 64)
	field := q.conditional[2]
	high, _ := strconv.ParseInt(q.conditional[4], 10, 64)

	if q.conditional[1] == "<" {
		low++
	}
	if q.conditional[3] == "<" {
		high--
	}

	elem := q.lastCallStackElem()
	elem.call.Args[field] = &Condition{Op: BETWEEN, Value: []interface{}{low, high}}

	q.conditional = nil
}

func (q *Query) addField(field string) {
	elem := q.lastCallStackElem()
	if elem == nil {
		panic(fmt.Sprintf("addField called with '%s' while element is nil", field))
	} else if elem.lastField != "" {
		panic(fmt.Sprintf("addField called with '%s' while field is not empty, it's: %s", field, elem.lastField))
	}
	elem.lastField = field
	if elem.call.Args == nil {
		elem.call.Args = make(map[string]interface{})
	}
}

// validateArgField ensures that field does not already
// exist as a key in the Args map before adding the new
// key/value.
func (q *Query) validateArgField(elem *callStackElem) {
	if _, exists := elem.call.Args[elem.lastField]; exists {
		panic(fmt.Sprintf("%s: %s", duplicateArgErrorMessage, elem.lastField))
	}
}

func (q *Query) addVal(val interface{}) {
	elem := q.lastCallStackElem()
	if elem == nil || elem.lastField == "" {
		panic(fmt.Sprintf("addVal called with '%s' when lastField is empty", val))
	}
	if elem.inList {
		list := elem.call.Args[elem.lastField].([]interface{})
		elem.call.Args[elem.lastField] = append(list, val)
		return
	}
	if elem.lastCond != ILLEGAL {
		q.validateArgField(elem) // case 1
		elem.call.Args[elem.lastField] = &Condition{
			Op:    elem.lastCond,
			Value: val,
		}
	} else {
		q.validateArgField(elem) // case 2
		elem.call.Args[elem.lastField] = val
	}
	elem.lastField = ""
	elem.lastCond = ILLEGAL
}

func (q *Query) addNumVal(val string) {
	elem := q.lastCallStackElem()
	if elem == nil || elem.lastField == "" {
		panic(fmt.Sprintf("addIntVal called with '%s' when lastField is empty", val))
	}
	var ival interface{}
	var err error
	if strings.Contains(val, ".") {
		ival, err = strconv.ParseFloat(val, 64)
	} else {
		ival, err = strconv.ParseInt(val, 10, 64)
	}
	if err != nil {
		panic(fmt.Sprintf("%s: %s", intOutOfRangeError, err))
	}
	if elem.inList {
		if elem.lastCond != ILLEGAL {
			list := elem.call.Args[elem.lastField].(*Condition).Value.([]interface{})
			elem.call.Args[elem.lastField] = &Condition{
				Op:    elem.lastCond,
				Value: append(list, ival),
			}
		} else {
			list := elem.call.Args[elem.lastField].([]interface{})
			elem.call.Args[elem.lastField] = append(list, ival)
		}
		return
	} else if elem.lastCond != ILLEGAL {
		q.validateArgField(elem) // case 3
		elem.call.Args[elem.lastField] = &Condition{
			Op:    elem.lastCond,
			Value: ival,
		}
	} else {
		q.validateArgField(elem) // case 4
		elem.call.Args[elem.lastField] = ival
	}
	elem.lastField = ""
	elem.lastCond = ILLEGAL
}

func (q *Query) startList() {
	elem := q.lastCallStackElem()
	q.validateArgField(elem) // case 5
	if elem.lastCond != ILLEGAL {
		elem.call.Args[elem.lastField] = &Condition{
			Op:    elem.lastCond,
			Value: make([]interface{}, 0),
		}
	} else {
		elem.call.Args[elem.lastField] = make([]interface{}, 0)
	}
	elem.inList = true
}

func (q *Query) endList() {
	elem := q.lastCallStackElem()
	elem.inList = false
	elem.lastField = ""
	elem.lastCond = ILLEGAL
}

func (q *Query) addGT() {
	q.lastCallStackElem().lastCond = GT
}
func (q *Query) addLT() {
	q.lastCallStackElem().lastCond = LT
}
func (q *Query) addGTE() {
	q.lastCallStackElem().lastCond = GTE
}
func (q *Query) addLTE() {
	q.lastCallStackElem().lastCond = LTE
}
func (q *Query) addEQ() {
	q.lastCallStackElem().lastCond = EQ
}
func (q *Query) addNEQ() {
	q.lastCallStackElem().lastCond = NEQ
}
func (q *Query) addBTWN() {
	q.lastCallStackElem().lastCond = BETWEEN
}

// WriteCallN returns the number of mutating calls.
func (q *Query) WriteCallN() int {
	var n int
	for _, call := range q.Calls {
		switch call.Name {
		case "Set", "Clear", "SetRowAttrs", "SetColumnAttrs":
			n++
		}
	}
	return n
}

// String returns a string representation of the query.
func (q *Query) String() string {
	a := make([]string, len(q.Calls))
	for i, call := range q.Calls {
		a[i] = call.String()
	}
	return strings.Join(a, "\n")
}

type callStackElem struct {
	call      *Call
	lastField string
	lastCond  Token
	inList    bool
}

// Some call types may require special handling, which needs to occur
// before distributing processing to individual shards.
type CallType byte

const (
	// Normal calls can be executed per shard.
	PrecallNone = CallType(iota)
	// PreCallGlobal indicates a call which must be run globally *before*
	// distributing the call to other shards. Example: A Distinct query,
	// where every shard could potentially produce results for any shard,
	// so you have to produce the results up front.
	PrecallGlobal
	// PreCallPerNode indicates a call which needs to be run per-shard
	// in a way that lets it be done on each shard, but where it should
	// be done prior to spawning per-shard goroutines. Example:
	// A cross-index query, where each local shard may or may not need
	// to get data from a remote node, but batches of shards can
	// probably be gotten from the same remote node.
	PrecallPerNode
)

// Call represents a function call in the AST. The Precomputed field
// is used by the executor to handle non-standard call types; it does
// these by actually executing them separately, then replacing them
// in the call tree with a new call using the special precomputed
// type, with the Precomputed field set to a map from shards to results.
type Call struct {
	Name        string
	Args        map[string]interface{}
	Children    []*Call
	Type        CallType
	Precomputed map[uint64]interface{}
}

// callInfo defines the arguments allowed for a particular PQL call, and
// possibly things about its semantics. If allowUnknown is true, unfamiliar
// non-reserved names are allowed on the assumption that they're field names.
// Otherwise, only those names explicitly listed are allowed. Reserved args
// (those with a leading underscore) are never allowed unless explicitly
// present.
//
// The prototypes map maps from argument names to a value. If the value is
// non-nil, the argument will be checked for type-matching. So, for instance,
// `x: 10` would indicate that x must be an int.
type callInfo struct {
	allowUnknown bool
	prototypes   map[string]interface{}
	callType     CallType
}

// We want to be able to accept either a string or int64 for
// field names. Special-case type:
type stringOrInt64Type struct{}

var stringOrInt64 stringOrInt64Type

var allowUnderField = callInfo{
	allowUnknown: true,
	prototypes: map[string]interface{}{
		"_field": "",
	},
}

var allowField = callInfo{
	allowUnknown: false,
	prototypes: map[string]interface{}{
		"field": "",
	},
}

var callInfoByFunc = map[string]callInfo{
	// the easy cases: things that take arbitrary inputs, because they're
	// taking field=value cases
	"Bitmap": {allowUnknown: true},
	"Count":  {allowUnknown: true},
	"Row":    {allowUnknown: true},
	"Range":  {allowUnknown: true},

	// allow only "field=X" cases with string field names
	"Max": allowField,
	"Min": allowField,
	"Sum": allowField,

	// only take other calls, should never have "args"
	"Difference": {allowUnknown: false},
	"Intersect":  {allowUnknown: false},
	"Not":        {allowUnknown: false},
	"ClearRow":   {allowUnknown: true},
	"Store":      {allowUnknown: true},
	"MinRow":     allowField,
	"MaxRow":     allowField,
	"Rows": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"_field":   "",
			"field":    "",
			"limit":    int64(0),
			"column":   nil,
			"previous": nil,
			"from":     nil,
			"to":       nil,
		},
	},
	"Shift": {allowUnknown: false,
		prototypes: map[string]interface{}{
			"n": int64(0),
		},
	},
	"Union": {allowUnknown: false},
	"Xor":   {allowUnknown: false},

	// things that take _field
	"TopN": allowUnderField,
	// special cases:
	"Clear": {
		allowUnknown: true,
		prototypes: map[string]interface{}{
			"_col": stringOrInt64,
		},
	},
	"GroupBy": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"filter":   nil,
			"limit":    int64(0),
			"previous": nil,
		},
	},
	"Options": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"excludeRowAttrs": true,
			"excludeColumns":  true,
			"columnAttrs":     true,
			"shards":          nil,
		},
	},
	"Set": {
		allowUnknown: true,
		prototypes: map[string]interface{}{
			"_col":       stringOrInt64,
			"_timestamp": "",
		},
	},
	"Precomputed": {
		allowUnknown: true,
	},
	"SetBit": {
		allowUnknown: true,
		prototypes: map[string]interface{}{
			"_col": stringOrInt64,
		},
	},
	"SetRowAttrs": {
		allowUnknown: true,
		prototypes: map[string]interface{}{
			"_field": "",
			"_row":   stringOrInt64,
		},
	},
	"SetColumnAttrs": {
		allowUnknown: true,
		prototypes: map[string]interface{}{
			"_field": "",
			"_col":   stringOrInt64,
		},
	},
}

// RegisterPluginFuncs adds arg validation for plugin funcs. Not very good
// arg validation.
func RegisterPluginFuncs(ops []ext.BitmapOp) {
	for _, op := range ops {
		// ignore overlap for now. This should change.
		if _, ok := callInfoByFunc[op.Name]; ok {
			continue
		}
		ci := callInfo{allowUnknown: true}
		if len(op.Reserved) > 0 {
			// mark these as valid/known reserved words
			ci.prototypes = make(map[string]interface{})
			for _, res := range op.Reserved {
				ci.prototypes[res] = nil
			}
		}
		t := op.Func.BitmapOpType()
		if t.Precall == ext.OpPrecallGlobal {
			ci.callType = PrecallGlobal
		}
		callInfoByFunc[op.Name] = ci
	}
}

// CheckCallInfo tries to validate that arguments are correct and valid for the
// given call. It does not guarantee checking all possible errors; for instance,
// if an argument is a field name, CheckCallInfo can't validate that the field
// exists. It also updates with information like whether the call is expected
// to require precalling.
func (c *Call) CheckCallInfo() error {
	valid, ok := callInfoByFunc[c.Name]
	if !ok {
		return fmt.Errorf("no arg validation for '%s'", c.Name)
	}
	c.Type = valid.callType
	for k, v := range c.Args {
		acceptable, ok := valid.prototypes[k]
		if !ok && !valid.allowUnknown {
			return fmt.Errorf("'%s': unknown arg '%s'", c.String(), k)
		}
		if !ok && strings.HasPrefix(k, "_") {
			return fmt.Errorf("'%s': unknown reserved arg '%s'", c.String(), k)
		}
		if acceptable == nil {
			continue
		}
		// if the types are identical, that's fine
		if reflect.TypeOf(acceptable) == reflect.TypeOf(v) {
			continue
		}
		if reflect.TypeOf(acceptable) == reflect.TypeOf(stringOrInt64) {
			switch v.(type) {
			case string, int64:
				continue
			default:
				return fmt.Errorf("'%s': arg '%s' needed a string or integer value, got %T.",
					c.String(), k, v)
			}
		}
		return fmt.Errorf("'%s': arg '%s' wrong type (got %T, expected %T)",
			c.String(), k, v, acceptable)
	}
	// call-specific checking
	for _, child := range c.Children {
		if err := child.CheckCallInfo(); err != nil {
			return err
		}
	}
	return nil
}

// FieldArg determines which key-value pair contains the field and rowID,
// in the case of arguments like Set(colID, field=rowID).
// Returns the field as a string if present, or an error if not.
func (c *Call) FieldArg() (string, error) {
	for arg := range c.Args {
		if !IsReservedArg(arg) {
			return arg, nil
		}
	}
	return "", fmt.Errorf("no field argument specified")
}

func IsReservedArg(name string) bool {
	if strings.HasPrefix(name, "_") {
		return true
	}
	switch name {
	case "from", "to", "index":
		return true
	default:
		return false
	}
}

// CallIndex handles guessing whether we've been asked to apply this to a
// different index. An empty string means "no".
func (c *Call) CallIndex() string {
	if index, ok := c.Args["_index"]; ok {
		if index, ok := index.(string); ok {
			return index
		}
	}
	if index, ok := c.Args["index"]; ok && index != "" {
		if index, ok := index.(string); ok {
			return index
		}
	}
	return ""
}

// BoolArg is for reading the value at key from call.Args as a bool. If the
// key is not in Call.Args, the value of the returned bool will be false, and
// the error will be nil. The value is assumed to be a bool. An error is
// returned if the value is not a bool.
func (c *Call) BoolArg(key string) (bool, bool, error) {
	val, ok := c.Args[key]
	if !ok {
		return false, false, nil
	}
	switch tval := val.(type) {
	case bool:
		return tval, true, nil
	default:
		return false, true, fmt.Errorf("could not convert %v of type %T to bool in Call.BoolArg", tval, tval)
	}
}

// UintArg is for reading the value at key from call.Args as a uint64. If the
// key is not in Call.Args, the value of the returned bool will be false, and
// the error will be nil. The value is assumed to be a uint64 or an int64 and
// then cast to a uint64. An error is returned if the value is not an int64 or
// uint64.
func (c *Call) UintArg(key string) (uint64, bool, error) {
	val, ok := c.Args[key]
	if !ok {
		return 0, false, nil
	}
	switch tval := val.(type) {
	case int64:
		if tval < 0 {
			return 0, true, fmt.Errorf("value for '%s' must be positive, but got %v", key, tval)
		}
		return uint64(tval), true, nil
	case uint64:
		return tval, true, nil
	default:
		return 0, true, fmt.Errorf("could not convert %v of type %T to uint64 in Call.UintArg", tval, tval)
	}
}

// IntArg is for reading the value at key from call.Args as an int64. If the
// key is not in Call.Args, the value of the returned bool will be false, and
// the error will be nil. The value is assumed to be a unt64 or an int64 and
// then cast to an int64. An error is returned if the value is not an int64 or
// uint64.
func (c *Call) IntArg(key string) (int64, bool, error) {
	val, ok := c.Args[key]
	if !ok {
		return 0, false, nil
	}
	switch tval := val.(type) {
	case int64:
		return tval, true, nil
	case uint64:
		return int64(tval), true, nil
	default:
		return 0, true, fmt.Errorf("could not convert %v of type %T to int64 in Call.IntArg", tval, tval)
	}
}

// UintSliceArg reads the value at key from call.Args as a slice of uint64. If
// the key is not in Call.Args, the value of the returned bool will be false,
// and the error will be nil. If the value is a slice of int64 it will convert
// it to []uint64. Otherwise, if it is not a []uint64 it will return an error.
func (c *Call) UintSliceArg(key string) ([]uint64, bool, error) {
	val, ok := c.Args[key]
	if !ok {
		return nil, false, nil
	}

	switch tval := val.(type) {
	case []uint64:
		return tval, true, nil
	case []int64:
		ret := make([]uint64, len(tval))
		for i, v := range tval {
			ret[i] = uint64(v)
		}
		return ret, true, nil
	default:
		return nil, true, fmt.Errorf("unexpected type %T in UintSliceArg, val %v", tval, tval)
	}
}

// CallArg is for reading the value at key from call.Args as a Call. If the
// key is not in Call.Args, the value of the returned value will be nil, and
// the error will be nil. An error is returned if the value is not a Call.
func (c *Call) CallArg(key string) (*Call, bool, error) {
	val, ok := c.Args[key]
	if !ok {
		return nil, false, nil
	}
	switch tval := val.(type) {
	case *Call:
		return tval, true, nil
	default:
		return nil, true, fmt.Errorf("could not convert %v of type %T to Call in Call.CallArg", tval, tval)
	}
}

// keys returns a list of argument keys in sorted order.
func (c *Call) keys() []string {
	a := make([]string, 0, len(c.Args))
	for k := range c.Args {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Clone returns a copy of c.
func (c *Call) Clone() *Call {
	if c == nil {
		return nil
	}

	other := &Call{
		Name: c.Name,
		Args: CopyArgs(c.Args),
	}
	if c.Children != nil {
		other.Children = make([]*Call, len(c.Children))
		for i := range c.Children {
			other.Children[i] = c.Children[i].Clone()
		}
	}
	return other
}

// String returns the string representation of the call.
func (c *Call) String() string {
	var buf bytes.Buffer

	// Write name.
	if c.Name != "" {
		buf.WriteString(c.Name)
	} else {
		buf.WriteString("!UNNAMED")
	}

	// Write opening.
	buf.WriteByte('(')

	// Write child list.
	for i, child := range c.Children {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(child.String())
	}

	// Separate children and args, if necessary.
	if len(c.Children) > 0 && len(c.Args) > 0 {
		buf.WriteString(", ")
	}

	// Write arguments in key order.
	for i, key := range c.keys() {
		if i > 0 {
			buf.WriteString(", ")
		}
		// If the Arg value is a Condition, then don't include
		// the equal sign in the string representation.
		switch v := c.Args[key].(type) {
		case *Condition:
			fmt.Fprintf(&buf, "%v %s", key, v.String())
		default:
			fmt.Fprintf(&buf, "%v=%s", key, formatValue(v))
		}
	}

	// Write closing.
	buf.WriteByte(')')

	return buf.String()
}

// HasConditionArg returns true if any arg is a conditional.
func (c *Call) HasConditionArg() bool {
	for _, v := range c.Args {
		if _, ok := v.(*Condition); ok {
			return true
		}
	}
	return false
}

// Condition represents an operation & value.
// When used in an argument map it represents a binary expression.
type Condition struct {
	Op    Token
	Value interface{}
}

// String returns the string representation of the condition.
func (cond *Condition) String() string {
	return fmt.Sprintf("%s %s", cond.Op.String(), formatValue(cond.Value))
}

// IntSliceValue reads cond.Value as a slice of uint64.
// If the value is a slice of uint64 it will convert
// it to []int64. Otherwise, if it is not a []int64 it will return an error.
func (cond *Condition) IntSliceValue() ([]int64, error) {
	val := cond.Value

	switch tval := val.(type) {
	case []interface{}:
		ret := make([]int64, len(tval))
		for i, v := range tval {
			switch tv := v.(type) {
			case int64:
				ret[i] = tv
			case uint64:
				ret[i] = int64(tv)
			default:
				return nil, fmt.Errorf("unexpected value type %T in IntSliceValue, val %v", tv, tv)
			}
		}
		return ret, nil
	default:
		return nil, fmt.Errorf("unexpected type %T in IntSliceValue, val %v", tval, tval)
	}
}

func formatValue(v interface{}) string {
	switch v := v.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	case []interface{}:
		return joinInterfaceSlice(v)
	case []uint64:
		return joinUint64Slice(v)
	case time.Time:
		return fmt.Sprintf("\"%s\"", v.Format(timeFormat))
	case *Condition:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// CopyArgs returns a copy of m.
func CopyArgs(m map[string]interface{}) map[string]interface{} {
	other := make(map[string]interface{}, len(m))
	for k, v := range m {
		other[k] = v
	}
	return other
}

func joinInterfaceSlice(a []interface{}) string {
	other := make([]string, len(a))
	for i := range a {
		switch v := a[i].(type) {
		case string:
			other[i] = fmt.Sprintf("%q", v)
		default:
			other[i] = fmt.Sprintf("%v", v)
		}
	}
	return "[" + strings.Join(other, ",") + "]"
}

func joinUint64Slice(a []uint64) string {
	other := make([]string, len(a))
	for i := range a {
		other[i] = strconv.FormatUint(a[i], 10)
	}
	return "[" + strings.Join(other, ",") + "]"
}
