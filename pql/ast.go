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
			layout := "2006-01-02T15:04:05"
			fmt.Fprintf(&buf, "%v=\"%s\"", key, v.Format(layout))
		default:
			fmt.Fprintf(&buf, "%v=%v", key, v)
		}
	}

	// Write closing.
	buf.WriteByte(')')

	return buf.String()
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

// Arg represents an call argument.
// The key can be the index or the string key.
// The value can be a uint64, []uint64, string, Call, or Calls.
type Arg struct {
	Key   interface{}
	Value interface{}
}
