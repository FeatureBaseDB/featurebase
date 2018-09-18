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
	"sort"
	"strconv"
	"strings"
	"time"
)

// Query represents a PQL query.
type Query struct {
	Calls []*Call

	lastField string
	lastCond  Token
	inList    bool
	callStack []*Call

	conditional []string
}

func (q *Query) startCall(name string) {
	newCall := &Call{Name: name}
	q.callStack = append(q.callStack, newCall)

	if len(q.callStack) == 1 {
		q.Calls = append(q.Calls, newCall)
	} else {
		calls := q.callStack[len(q.callStack)-2].Children
		q.callStack[len(q.callStack)-2].Children = append(calls, newCall)
	}
}

func (q *Query) endCall() {
	q.callStack = q.callStack[:len(q.callStack)-1]
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
	call := q.callStack[len(q.callStack)-1]
	if call.Args == nil {
		call.Args = make(map[string]interface{})
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
	if q.conditional[3] == "<=" {
		high++
	}

	call := q.callStack[len(q.callStack)-1]
	call.Args[field] = &Condition{Op: BETWEEN, Value: []interface{}{low, high}}

	q.conditional = nil
}

func (q *Query) addField(field string) {
	if q.lastField != "" {
		panic(fmt.Sprintf("addField called with '%s' while field is not empty, it's: %s", field, q.lastField))
	}
	q.lastField = field
	call := q.callStack[len(q.callStack)-1]
	if call.Args == nil {
		call.Args = make(map[string]interface{})
	}
}

func (q *Query) addVal(val interface{}) {
	if q.lastField == "" {
		panic(fmt.Sprintf("addVal called with '%s' when lastField is empty", val))
	}
	call := q.callStack[len(q.callStack)-1]
	if q.inList {
		list := call.Args[q.lastField].([]interface{})
		call.Args[q.lastField] = append(list, val)
		return
	}
	if q.lastCond != ILLEGAL {
		call.Args[q.lastField] = &Condition{
			Op:    q.lastCond,
			Value: val,
		}
	} else {
		call.Args[q.lastField] = val
	}
	q.lastField = ""
	q.lastCond = ILLEGAL
}

func (q *Query) addNumVal(val string) {
	if q.lastField == "" {
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
		panic(err)
	}
	call := q.callStack[len(q.callStack)-1]
	if q.inList {
		if q.lastCond != ILLEGAL {
			list := call.Args[q.lastField].(*Condition).Value.([]interface{})
			call.Args[q.lastField] = &Condition{
				Op:    q.lastCond,
				Value: append(list, ival),
			}
		} else {
			list := call.Args[q.lastField].([]interface{})
			call.Args[q.lastField] = append(list, ival)
		}
		return
	} else if q.lastCond != ILLEGAL {
		call.Args[q.lastField] = &Condition{
			Op:    q.lastCond,
			Value: ival,
		}
	} else {
		call.Args[q.lastField] = ival
	}
	q.lastField = ""
	q.lastCond = ILLEGAL
}

func (q *Query) startList() {
	call := q.callStack[len(q.callStack)-1]
	if q.lastCond != ILLEGAL {
		call.Args[q.lastField] = &Condition{
			Op:    q.lastCond,
			Value: make([]interface{}, 0),
		}
	} else {
		call.Args[q.lastField] = make([]interface{}, 0)
	}
	q.inList = true
}

func (q *Query) endList() {
	q.inList = false
	q.lastField = ""
	q.lastCond = ILLEGAL
}

func (q *Query) addGT() {
	q.lastCond = GT
}
func (q *Query) addLT() {
	q.lastCond = LT
}
func (q *Query) addGTE() {
	q.lastCond = GTE
}
func (q *Query) addLTE() {
	q.lastCond = LTE
}
func (q *Query) addEQ() {
	q.lastCond = EQ
}
func (q *Query) addNEQ() {
	q.lastCond = NEQ
}
func (q *Query) addBTWN() {
	q.lastCond = BETWEEN
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

// Call represents a function call in the AST.
type Call struct {
	Name     string
	Args     map[string]interface{}
	Children []*Call
}

// FieldArg determines which key-value pair contains the field and rowID,
// in the case of arguments like Set(colID, field=rowID).
// Returns the field as a string if present, or an error if not.
func (c *Call) FieldArg() (string, error) {
	for arg := range c.Args {
		if !strings.HasPrefix(arg, "_") {
			return arg, nil
		}
	}
	return "", fmt.Errorf("No field argument specified")
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
