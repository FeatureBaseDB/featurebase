package pql

import (
	"fmt"
	"strings"
	"time"
)

// Query represents a PQL query.
type Query struct {
	Root Call
}

// String returns a string representation of the query.
func (q *Query) String() string { return q.Root.String() }

// Node represents any node in the AST.
type Node interface {
	node()
	String() string
}

func (*Clear) node()      {}
func (*Count) node()      {}
func (*Difference) node() {}
func (*Get) node()        {}
func (*Intersect) node()  {}
func (*Range) node()      {}
func (*Set) node()        {}
func (*TopN) node()       {}
func (*Union) node()      {}

// Call represents a function call in the AST.
type Call interface {
	Node
	call()
}

func (*Clear) call()      {}
func (*Count) call()      {}
func (*Difference) call() {}
func (*Get) call()        {}
func (*Intersect) call()  {}
func (*Range) call()      {}
func (*Set) call()        {}
func (*TopN) call()       {}
func (*Union) call()      {}

// Calls represents a list of calls.
type Calls []Call

// String returns a string representation of the calls as a comma-delimited list.
func (a Calls) String() string {
	args := make([]string, len(a))
	for i, c := range a {
		args[i] = c.String()
	}
	return strings.Join(args, ", ")
}

// BitmapCall represents a function call that returns a bitmap.
type BitmapCall interface {
	Call
	bitmapCall()
}

// BitmapCalls represents a list of bitmap calls.
type BitmapCalls []BitmapCall

// String returns a string representation of the calls as a comma-delimited list.
func (a BitmapCalls) String() string {
	args := make([]string, len(a))
	for i, c := range a {
		args[i] = c.String()
	}
	return strings.Join(args, ", ")
}

func (*Difference) bitmapCall() {}
func (*Get) bitmapCall()        {}
func (*Intersect) bitmapCall()  {}
func (*Range) bitmapCall()      {}
func (*Union) bitmapCall()      {}

// Clear represents a clear() function call.
type Clear struct {
	ID        uint64
	Frame     string
	Filter    uint64
	ProfileID uint64
}

// String returns the string representation of the call.
func (c *Clear) String() string {
	args := make([]string, 0, 4)
	if c.ID != 0 {
		args = append(args, fmt.Sprintf("id=%d", c.ID))
	}
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	if c.Filter != 0 {
		args = append(args, fmt.Sprintf("filter=%d", c.Filter))
	}
	if c.ProfileID != 0 {
		args = append(args, fmt.Sprintf("profile_id=%d", c.ProfileID))
	}
	return fmt.Sprintf("clear(%s)", strings.Join(args, ", "))
}

// Count represents a count() function call.
type Count struct {
	Input BitmapCall
}

// String returns the string representation of the call.
func (c *Count) String() string {
	return fmt.Sprintf("count(%s)", c.Input.String())
}

// Difference represents an difference() function call.
type Difference struct {
	Inputs BitmapCalls
}

// String returns the string representation of the call.
func (c *Difference) String() string {
	return fmt.Sprintf("difference(%s)", c.Inputs.String())
}

// Get represents a get() function call.
type Get struct {
	ID    uint64
	Frame string
}

// String returns the string representation of the call.
func (c *Get) String() string {
	args := make([]string, 0, 2)
	if c.ID != 0 {
		args = append(args, fmt.Sprintf("id=%d", c.ID))
	}
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	return fmt.Sprintf("get(%s)", strings.Join(args, ", "))
}

// Intersect represents an intersect() function call.
type Intersect struct {
	Inputs BitmapCalls
}

// String returns the string representation of the call.
func (c *Intersect) String() string {
	return fmt.Sprintf("intersect(%s)", c.Inputs.String())
}

// Range represents a range() function call.
type Range struct {
	ID        uint64
	Frame     string
	StartTime time.Time
	EndTime   time.Time
}

// String returns the string representation of the call.
func (c *Range) String() string {
	args := make([]string, 0, 2)
	if c.ID != 0 {
		args = append(args, fmt.Sprintf("id=%d", c.ID))
	}
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	if !c.StartTime.IsZero() {
		args = append(args, fmt.Sprintf("start=%s", c.StartTime.Format(TimeFormat)))
	}
	if !c.EndTime.IsZero() {
		args = append(args, fmt.Sprintf("end=%s", c.EndTime.Format(TimeFormat)))
	}
	return fmt.Sprintf("range(%s)", strings.Join(args, ", "))
}

// Set represents a set() function call.
type Set struct {
	ID        uint64
	Frame     string
	Filter    uint64
	ProfileID uint64
}

// String returns the string representation of the call.
func (c *Set) String() string {
	args := make([]string, 0, 2)
	if c.ID != 0 {
		args = append(args, fmt.Sprintf("id=%d", c.ID))
	}
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	if c.Filter != 0 {
		args = append(args, fmt.Sprintf("filter=%d", c.Filter))
	}
	if c.ProfileID != 0 {
		args = append(args, fmt.Sprintf("profile_id=%d", c.ProfileID))
	}
	return fmt.Sprintf("set(%s)", strings.Join(args, ", "))
}

// TopN represents a top-n() function call.
type TopN struct {
	Frame string
	N     int
}

// String returns the string representation of the call.
func (c *TopN) String() string { panic("FIXME") }

// Union represents a union() function call.
type Union struct {
	Inputs BitmapCalls
}

// String returns the string representation of the call.
func (c *Union) String() string {
	return fmt.Sprintf("union(%s)", c.Inputs.String())
}
