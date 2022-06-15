// Copyright 2021 Molecula Corp. All rights reserved.
package pql

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Query represents a PQL query.
type Query struct {
	Calls []*Call

	callStack   []*callStackElem
	conditional []string
}

// ExpandVars recursively replaces variables in the query with their values.
func (q *Query) ExpandVars(vars map[string]interface{}) (*Query, error) {
	other := *q
	other.Calls = make([]*Call, 0, len(q.Calls))

	for _, c := range q.Calls {
		newCalls, err := c.ExpandVars(vars)
		if err != nil {
			return nil, err
		}
		if len(newCalls) == 0 {
			return nil, fmt.Errorf("no values to use for variable expansion")
		}
		other.Calls = append(other.Calls, newCalls...)
	}

	return &other, nil
}

// HasCall returns true if q contains the given call name.
func (q *Query) HasCall(name string) bool {
	for _, c := range q.Calls {
		if c.HasCall(name) {
			return true
		}
	}
	return false
}

func (q *Query) startCall(name string) {
	// Coerce every name into a canonical form if we know of one.
	if canon, ok := canonicalCaps[strings.ToLower(name)]; ok {
		name = canon
	}
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
	if key == "field" {
		q.addField("_field")
	} else {
		q.addField(key)
	}
	q.addNumVal(value)
}

func (q *Query) addPosStr(key, value string) {
	q.addField(key)
	if strings.HasPrefix(value, "$") {
		q.addVal(NewVariable(strings.TrimPrefix(value, "$")))
	} else {
		q.addVal(value)
	}
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
	low := parseNum(q.conditional[0])
	field := q.conditional[2]
	high := parseNum(q.conditional[4])

	var op Token
	switch q.conditional[1] + q.conditional[3] {
	case "<<":
		op = BTWN_LT_LT
	case "<=<":
		op = BTWN_LTE_LT
	case "<<=":
		op = BTWN_LT_LTE
	case "<=<=":
		op = BETWEEN
	default:
		panic(fmt.Sprintf("impossible conditional ops: '%s' and '%s'", q.conditional[1], q.conditional[3]))
	}

	elem := q.lastCallStackElem()
	elem.call.Args[field] = &Condition{Op: op, Value: []interface{}{low, high}}

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
	if vs, ok := val.(string); ok {
		vsu, err := Unquote(vs)
		if err != nil {
			panic(err)
		}
		val = vsu
	}
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
	ival := parseNum(val)
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

func (q *Query) addTimestampVal(val string) {
	elem := q.lastCallStackElem()
	if elem == nil || elem.lastField == "" {
		panic(fmt.Sprintf("addTimestampVal called with '%s' when lastField is empty", val))
	}
	tsval := parseTimestamp(val)
	if elem.inList {
		if elem.lastCond != ILLEGAL {
			list := elem.call.Args[elem.lastField].(*Condition).Value.([]interface{})
			elem.call.Args[elem.lastField] = &Condition{
				Op:    elem.lastCond,
				Value: append(list, tsval),
			}
		} else {
			list := elem.call.Args[elem.lastField].([]interface{})
			elem.call.Args[elem.lastField] = append(list, tsval)
		}
		return
	} else if elem.lastCond != ILLEGAL {
		q.validateArgField(elem) // case 3
		elem.call.Args[elem.lastField] = &Condition{
			Op:    elem.lastCond,
			Value: tsval,
		}
	} else {
		q.validateArgField(elem) // case 4
		elem.call.Args[elem.lastField] = tsval
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
		case "Set", "Clear", "ClearRow", "Store", "SetBit":
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
	// PrecallNone calls can be executed per shard.
	PrecallNone = CallType(iota)

	// PrecallGlobal indicates a call which must be run globally *before*
	// distributing the call to other shards. Example: A Distinct query,
	// where every shard could potentially produce results for any shard,
	// so you have to produce the results up front.
	// These are processed directly when inside of a count operation.
	PrecallGlobal

	// PrecallPerNode indicates a call which needs to be run per-shard
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

// HasCall returns true if q contains the given call name.
func (c *Call) HasCall(name string) bool {
	if c.Name == name {
		return true
	}

	for _, child := range c.Children {
		if child.HasCall(name) {
			return true
		}
	}
	return false
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

// We want to be able to accept either a string or variable for
// _field args. Special-case type:
type stringOrVariableType struct{}

var stringOrVariable stringOrVariableType

// We want to be able to accept either a interface or variable for
// column args. Special-case type:
type interfaceOrVariableType struct{}

var interfaceOrVariable interfaceOrVariableType

var allowField = callInfo{
	allowUnknown: false,
	prototypes: map[string]interface{}{
		"_field": stringOrVariable,
		"field":  stringOrVariable,
	},
}

var callInfoByFunc = map[string]callInfo{
	// the easy cases: things that take arbitrary inputs, because they're
	// taking field=value cases
	"Bitmap": {allowUnknown: true},
	"Count":  {allowUnknown: true},
	"Delete": {allowUnknown: true},
	"Row":    {allowUnknown: true},
	"Range":  {allowUnknown: true},

	"Distinct":  {allowUnknown: true, callType: PrecallGlobal},
	"Condition": {allowUnknown: true},

	// allow only "field=X" cases with string field names
	"Max": allowField,
	"Min": allowField,
	"Sum": allowField,

	// only take other calls, should never have "args"
	"Difference": {allowUnknown: false},
	"Intersect":  {allowUnknown: false},
	"Not":        {allowUnknown: false},
	"FieldValue": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"field":  "",
			"column": stringOrInt64,
		},
	},
	"All": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"limit":  int64(0),
			"offset": int64(0),
		},
	},
	"ClearRow": {allowUnknown: true},
	"Store":    {allowUnknown: true},
	"MinRow":   allowField,
	"MaxRow":   allowField,
	"Rows": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"_field":   stringOrVariable,
			"field":    stringOrVariable,
			"limit":    int64(0),
			"column":   nil,
			"previous": nil,
			"from":     nil,
			"to":       nil,
			"like":     "",
			"valueidx": int64(0),
			"in":       nil,
		},
	},
	"InnerUnionRows": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"_field": stringOrVariable,
			"field":  stringOrVariable,
			"from":   nil,
			"to":     nil,
			"rows":   nil,
		},
	},
	"Shift": {allowUnknown: false,
		prototypes: map[string]interface{}{
			"n": int64(0),
		},
	},
	"Union":     {allowUnknown: false},
	"UnionRows": {allowUnknown: false, callType: PrecallGlobal},
	"Extract":   {allowUnknown: false},
	"ExternalLookup": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"query": "",
			"write": true,
		},
	},
	"Limit": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"limit":  int64(0),
			"offset": int64(0),
		},
		callType: PrecallGlobal,
	},
	"Xor": {allowUnknown: false},

	"ConstRow": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"columns": interfaceOrVariable,
		},
		callType: PrecallGlobal,
	},

	"TopK": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"_field": stringOrVariable,
			"field":  stringOrVariable,
			"k":      int64(0),
			"filter": nil,
			"from":   nil,
			"to":     nil,
		},
	},

	"TopN": {
		allowUnknown: true,
		prototypes: map[string]interface{}{
			"_field": stringOrVariable,
			"field":  stringOrVariable,
		},
	},
	"Percentile": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"field":  stringOrVariable,
			"_field": stringOrVariable,
			"filter": nil,
			"nth":    nil,
		},
	},
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
			"filter":    nil,
			"limit":     int64(0),
			"offset":    int64(0),
			"previous":  nil,
			"aggregate": nil,
			"having":    nil,
			"sort":      "",
		},
	},
	"Options": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"shards": nil,
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
	"IncludesColumn": {
		allowUnknown: false,
		prototypes: map[string]interface{}{
			"column": stringOrInt64,
		},
	},
}

// We want to allow case-insensitive names, but we want to continue using
// friendly easy-to-read names like "SetBit", not "setbit". So,
// we make a map; put in a ToLower() string, get back the canonical
// capitalization. This might not have seemed like the best strategy if we
// didn't already have so much code relying on the exact strings.
var canonicalCaps = makeCanonicalMap(callInfoByFunc)

func makeCanonicalMap(from map[string]callInfo) map[string]string {
	m := make(map[string]string, len(from))
	for k := range from {
		m[strings.ToLower(k)] = k
	}
	return m
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
		if call, ok := v.(*Call); ok {
			if err := call.CheckCallInfo(); err != nil {
				return err
			}
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
				return fmt.Errorf("'%s': arg '%s' needed a string or integer value, got %T",
					c.String(), k, v)
			}
		}
		if reflect.TypeOf(acceptable) == reflect.TypeOf(stringOrVariable) {
			switch v.(type) {
			case string, *Variable:
				continue
			default:
				return fmt.Errorf("'%s': arg '%s' needed a string or variable value, got %T",
					c.String(), k, v)
			}
		}
		if reflect.TypeOf(acceptable) == reflect.TypeOf(interfaceOrVariable) {
			switch v.(type) {
			case []interface{}, *Variable:
				continue
			default:
				return fmt.Errorf("'%s': arg '%s' needed a []interface{} or variable value, got %T",
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

// Arg is for reading the value at key from call.Args.
// If the key is not in Call.Args, the value of the returned bool will be false.
func (c *Call) Arg(key string) (interface{}, bool) {
	v, ok := c.Args[key]
	return v, ok
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
	case []interface{}:
		ret := make([]uint64, len(tval))
		for i, v := range tval {
			if uv, ok := v.(uint64); ok {
				ret[i] = uv
			} else if iv, ok := v.(int64); ok && iv >= 0 {
				ret[i] = uint64(iv)
			} else {
				return nil, true, errors.Errorf("'%v' at position %d is %[1]T, but need positive integer", v, i)
			}
		}
		return ret, true, nil
	default:
		return nil, true, fmt.Errorf("unexpected type %T in UintSliceArg, val %v", tval, tval)
	}
}

func (c *Call) StringArg(key string) (string, bool, error) {
	val, ok := c.Args[key]
	if !ok {
		return "", false, nil
	}
	switch tval := val.(type) {
	case string:
		return tval, true, nil
	default:
		return "", true, fmt.Errorf("unexpected type %T in StringArg, val %v", tval, tval)
	}
}

func (c *Call) FirstStringArg(keys ...string) (string, error) {
	for _, k := range keys {
		val, ok, err := c.StringArg(k)
		if err != nil {
			return "", err
		}
		if !ok {
			continue
		}
		return val, nil
	}

	return "", fmt.Errorf("keys: %v not found", keys)
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
	// @seebs "...it should be safe,
	// because nothing should be writing to Precomputed
	// once it's gotten created in the first place."
	other.Precomputed = c.Precomputed

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
			fmt.Fprintf(&buf, "%s", v.StringWithSubj(key))
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

// TranslateInfo returns the relevant translation fields.
func (c *Call) TranslateInfo(columnLabel, rowLabel string) (colKey, rowKey, fieldName string) {
	switch c.Name {
	case "Set", "Clear", "Row", "Range", "ClearRow":
		// Positional args in new PQL syntax require special handling here.
		fieldName, _ = c.FieldArg()
		return "_" + columnLabel, fieldName, fieldName
	case "Rows":
		return "column", "previous", c.ArgString("_field")
	case "IncludesColumn":
		return "column", "", ""
	case "GroupBy":
		return "", "", ""
	default:
		return "col", "row", c.ArgString("_field")
	}
}

// Writable returns true if call is mutable (e.g. can write new translation keys)
func (c *Call) Writable() bool {
	switch c.Name {
	case "Set", "SetBit":
		return true
	case "Not":
		// to support queries like Not(Row(f="garbage"))
		return true
	default:
		return false
	}
}

func (c *Call) ArgString(key string) string {
	value, ok := c.Args[key]
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

// ExpandVars recursively replaces variables in the call with their values.
func (c *Call) ExpandVars(vars map[string]interface{}) ([]*Call, error) {
	switch c.Name {
	case "Row", "ConstRow", "Rows":
		for argK, argV := range c.Args {
			variable := getVariable(argV)
			if variable == nil {
				continue
			}
			for varK, varV := range vars {
				if variable.Name != varK {
					continue
				}
				switch values := varV.(type) {
				case []interface{}:
					return c.expandVars(argK, values), nil
				default:
					return nil, fmt.Errorf("expected variable value of type []interface{}, got: %T", values)
				}

			}
		}
		return []*Call{c}, nil
	default:
		other := *c
		other.Args = CopyArgs(c.Args)
		other.Children = make([]*Call, 0, len(c.Children))
		for _, child := range c.Children {
			newChildren, err := child.ExpandVars(vars)
			if err != nil {
				return nil, err
			}
			other.Children = append(other.Children, newChildren...)
		}
		for key, val := range other.Args {
			switch call := val.(type) {
			case *Call:
				newArg, err := call.ExpandVars(vars)
				if err != nil {
					return nil, err
				}
				switch len(newArg) {
				case 0:
					if call.Name == "Row" {
						other.Args[key] = &Call{Name: "All"}
					} else {
						return nil, fmt.Errorf("variable: non-Row calls require values be supplied, got: %+v", newArg)
					}
				case 1:
					other.Args[key] = newArg[0]
				default:
					return nil, fmt.Errorf("variable: requires single value for argument, got: %+v", newArg)
				}

			}
		}

		// if the call had children, but due to variable expansion, it now has none - then the user
		// did not select any values for any variables. If the child was originally a Row call, we equate
		// this to an All() (i.e. the user chooses to apply no conditions to the query).
		if len(c.Children) > 0 && len(other.Children) == 0 {
			if c.Children[0].Name == "Row" {
				r := Call{Name: "All"}
				other.Children = []*Call{&r}
			} else {
				return nil, fmt.Errorf("variable: non-Row calls require values be supplied")
			}
		}
		return []*Call{&other}, nil
	}
}

// expandVars specifies the implementation for variable expansion for various Call types
func (c *Call) expandVars(name string, values []interface{}) []*Call {
	switch c.Name {
	case "Row":
		if len(values) == 0 {
			cc := &Call{Name: "All"}
			return []*Call{cc}
		}
		union := &Call{Name: "Union"}
		for i := range values {
			r := Call{Name: "Row", Args: CopyArgs(c.Args)}
			switch cond := r.Args[name].(type) {
			case *Condition:
				r.Args[name] = &Condition{Op: cond.Op, Value: values[i]}
			default:
				r.Args[name] = values[i]
			}
			union.Children = append(union.Children, &r)
		}
		return []*Call{union}
	case "Rows":
		rows := make([]*Call, 0, len(values))
		for i := range values {
			r := Call{Name: "Rows"}
			r.Args = CopyArgs(c.Args)
			r.Args[name] = values[i]
			rows = append(rows, &r)
		}
		return rows
	case "ConstRow":
		r := Call{Name: "ConstRow"}
		r.Args = CopyArgs(c.Args)
		r.Args[name] = values
		return []*Call{&r}
	}
	return []*Call{c}
}

// getVariable returns *Variable given a Call argument if present
func getVariable(i interface{}) *Variable {
	switch _var := i.(type) {
	case *Condition:
		if v, ok := _var.Value.(*Variable); ok {
			return v
		}
		return nil
	case *Variable: // if interface{} is of type Variable
		return _var
	default:
		return nil
	}
}

// Condition represents an operation & value.
// When used in an argument map it represents a binary expression.
type Condition struct {
	Op    Token
	Value interface{}
}

// String returns the string representation of the condition.
func (cond *Condition) String() string {
	return fmt.Sprintf("%s%s", cond.Op.String(), formatValue(cond.Value))
}

// StringWithSubj returns the string representation of the condition
// including the provided subject.
func (cond *Condition) StringWithSubj(subj string) string {
	switch cond.Op {
	case EQ, NEQ, LT, LTE, GT, GTE:
		return fmt.Sprintf("%s%s", subj, cond.String())
	case BETWEEN, BTWN_LT_LTE, BTWN_LTE_LT, BTWN_LT_LT:
		val, ok := cond.StringSliceValue()
		if !ok || len(val) < 2 {
			return ""
		}
		if cond.Op == BETWEEN {
			return fmt.Sprintf("%s<=%s<=%s", val[0], subj, val[1])
		} else if cond.Op == BTWN_LT_LTE {
			return fmt.Sprintf("%s<%s<=%s", val[0], subj, val[1])
		} else if cond.Op == BTWN_LTE_LT {
			return fmt.Sprintf("%s<=%s<%s", val[0], subj, val[1])
		} else if cond.Op == BTWN_LT_LT {
			return fmt.Sprintf("%s<%s<%s", val[0], subj, val[1])
		}
	}
	return ""
}

func (cond *Condition) Uint64Value() (uint64, bool) {
	val := cond.Value

	switch tval := val.(type) {
	case int64:
		if tval >= 0 {
			return uint64(tval), true
		}
	case uint64:
		return tval, true
	}

	return 0, false
}

func (cond *Condition) Uint64SliceValue() ([]uint64, bool) {
	val := cond.Value

	switch tval := val.(type) {
	case []interface{}:
		ret := make([]uint64, len(tval))
		for i, v := range tval {
			switch tv := v.(type) {
			case int64:
				ret[i] = uint64(tv)
			case uint64:
				ret[i] = tv
			default:
				return nil, false
			}
		}
		return ret, true
	}

	return nil, false
}

func (cond *Condition) Int64Value() (int64, bool) {
	val := cond.Value

	switch tval := val.(type) {
	case int64:
		return tval, true
	case uint64:
		// TODO: consider overflow?
		return int64(tval), true
	}

	return 0, false
}

func (cond *Condition) Int64SliceValue() ([]int64, bool) {
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
				return nil, false
			}
		}
		return ret, true
	}

	return nil, false
}

// StringSliceValue returns the value(s) of the conditional
// as a slice of strings. For example, if cond.Value is
// []int64{-10,20}, this will return []string{"-10","20"}.
// It also returns a bool indicating that the conversion
// succeeded.
func (cond *Condition) StringSliceValue() ([]string, bool) {
	val := cond.Value

	switch tval := val.(type) {
	case []interface{}:
		ret := make([]string, len(tval))
		for i, v := range tval {
			switch tv := v.(type) {
			case int64:
				ret[i] = strconv.FormatInt(tv, 10)
			case uint64:
				ret[i] = strconv.FormatUint(tv, 10)
			case Decimal:
				ret[i] = tv.String()
			default:
				return nil, false
			}
		}
		return ret, true
	}

	return nil, false
}

// Variable represents a placeholder variable in a query.
type Variable struct {
	Name string
}

// NewVariable returns a new instance of Variable.
func NewVariable(name string) *Variable {
	return &Variable{Name: name}
}

// String returns the string representation of v.
func (v *Variable) String() string {
	return "$" + v.Name
}

func formatValue(v interface{}) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case string:
		return fmt.Sprintf("%q", v)
	case []interface{}:
		return joinInterfaceSlice(v)
	case []uint64:
		return joinUint64Slice(v)
	case time.Time:
		return fmt.Sprintf("\"%s\"", v.Format(time.RFC3339Nano))
	case *Condition:
		return v.String()
	case *Variable:
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

// CopyArgsDecimalToFloat makes a copy of m, but in the process,
// replaces any Decimal values with Float64 values.
func CopyArgsDecimalToFloat(m map[string]interface{}) map[string]interface{} {
	other := make(map[string]interface{}, len(m))
	for k, v := range m {
		if dec, ok := v.(Decimal); ok {
			other[k] = dec.Float64()
		} else {
			other[k] = v
		}
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

func parseNum(val string) interface{} {
	var ival interface{}
	var err error
	if strings.Contains(val, ".") {
		ival, err = ParseDecimal(val)
	} else {
		ival, err = strconv.ParseInt(val, 10, 64)
	}
	if err != nil {
		panic(fmt.Sprintf("%s: %s", intOutOfRangeError, err))
	}
	return ival
}

func parseTimestamp(val string) time.Time {
	tsval, err := time.Parse(time.RFC3339Nano, val)
	if err != nil {
		panic(fmt.Sprintf("%s: %s", invalidTimestampError, err))
	}
	return tsval
}
