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
}

// WriteCallN returns the number of mutating calls.
func (q *Query) WriteCallN() int {
	var n int
	for _, call := range q.Calls {
		switch call.Name {
		case "SetBit", "ClearBit", "SetRowAttrs", "SetColumnAttrs":
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

// Keys returns a list of argument keys in sorted order.
func (c *Call) Keys() []string {
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
	for i, key := range c.Keys() {
		if i > 0 {
			buf.WriteString(", ")
		}

		switch v := c.Args[key].(type) {
		case string:
			fmt.Fprintf(&buf, "%v=%q", key, v)
		case []interface{}:
			fmt.Fprintf(&buf, "%v=%s", key, joinInterfaceSlice(v))
		case []uint64:
			fmt.Fprintf(&buf, "%v=%s", key, joinUint64Slice(v))
		case time.Time:
			fmt.Fprintf(&buf, "%v=\"%s\"", key, v.Format(TimeFormat))
		default:
			fmt.Fprintf(&buf, "%v=%v", key, v)
		}
	}

	// Write closing.
	buf.WriteByte(')')

	return buf.String()
}

// SupportsInverse indicates that the call may be on an inverse frame.
func (c *Call) SupportsInverse() bool {
	return c.Name == "Bitmap" || c.Name == "TopN"
}

// IsInverse specifies if the call is for an inverse view.
// Return defaults to false unless absolutely sure of inversion.
func (c *Call) IsInverse(rowLabel, columnLabel string) bool {
	if c.SupportsInverse() {
		// Top-n has an explicit inverse flag.
		if c.Name == "TopN" {
			inverse, _ := c.Args["inverse"].(bool)
			return inverse
		}

		// Bitmap calls use the row/column labels to determine whether inverse.
		_, rowOK, rowErr := c.UintArg(rowLabel)
		_, columnOK, columnErr := c.UintArg(columnLabel)
		if rowErr != nil || columnErr != nil {
			return false
		}
		if !rowOK && columnOK {
			return true
		}
	}
	return false
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

// ExternalCall represents a function call to an external plugin.
type ExternalCall struct {
	Name string
	Args []Arg
}

// String returns the string representation of the call.
func (c *ExternalCall) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name)
	buf.WriteByte('(')

	// Write arg list.
	for i, arg := range c.Args {
		if i != 0 {
			buf.WriteString(", ")
		}

		// Write key if argument is keyed.
		if key, ok := arg.Key.(string); ok {
			buf.WriteString(key)
			buf.WriteByte('=')
		}

		// Write value based on its data type.
		switch v := arg.Value.(type) {
		case string:
			fmt.Fprintf(&buf, "%q", v)
		case Call:
			buf.WriteString(v.String())
		default:
			fmt.Fprintf(&buf, "%v", v)
		}
	}

	buf.WriteByte(')')
	return buf.String()
}

// Arg represents a call argument.
// The key can be the index or the string key.
// The value can be a uint64, []uint64, string, Call, or Calls.
type Arg struct {
	Key   interface{}
	Value interface{}
}
