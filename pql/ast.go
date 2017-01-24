package pql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Query represents a PQL query.
type Query struct {
	Calls Calls
}

// String returns a string representation of the query.
func (q *Query) String() string {
	a := make([]string, len(q.Calls))
	for i, call := range q.Calls {
		a[i] = call.String()
	}
	return strings.Join(a, "\n")
}

// Node represents any node in the AST.
type Node interface {
	node()
	String() string
}

func (*Bitmap) node()          {}
func (*ClearBit) node()        {}
func (*Count) node()           {}
func (*Difference) node()      {}
func (*Intersect) node()       {}
func (*Profile) node()         {}
func (*Range) node()           {}
func (*SetBit) node()          {}
func (*SetBitmapAttrs) node()  {}
func (*SetProfileAttrs) node() {}
func (*TopN) node()            {}
func (*Union) node()           {}

// Call represents a function call in the AST.
type Call interface {
	Node
	call()
}

func (*Bitmap) call()          {}
func (*ClearBit) call()        {}
func (*Count) call()           {}
func (*Difference) call()      {}
func (*Intersect) call()       {}
func (*Profile) call()         {}
func (*Range) call()           {}
func (*SetBit) call()          {}
func (*SetBitmapAttrs) call()  {}
func (*SetProfileAttrs) call() {}
func (*TopN) call()            {}
func (*Union) call()           {}

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
func (*Bitmap) bitmapCall()     {}
func (*Intersect) bitmapCall()  {}
func (*Range) bitmapCall()      {}
func (*Union) bitmapCall()      {}

// Bitmap represents a Bitmap() function call.
type Bitmap struct {
	ID    uint64
	Frame string
}

// String returns the string representation of the call.
func (c *Bitmap) String() string {
	args := make([]string, 0, 2)
	if c.ID != 0 {
		args = append(args, fmt.Sprintf("id=%d", c.ID))
	}
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	return fmt.Sprintf("Bitmap(%s)", strings.Join(args, ", "))
}

// ClearBit represents a ClearBit() function call.
type ClearBit struct {
	ID        uint64
	Frame     string
	ProfileID uint64
}

// String returns the string representation of the call.
func (c *ClearBit) String() string {
	args := make([]string, 0, 4)
	args = append(args, fmt.Sprintf("id=%d", c.ID))
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	if c.ProfileID != 0 {
		args = append(args, fmt.Sprintf("profileID=%d", c.ProfileID))
	}
	return fmt.Sprintf("ClearBit(%s)", strings.Join(args, ", "))
}

// Count represents a count() function call.
type Count struct {
	Input BitmapCall
}

// String returns the string representation of the call.
func (c *Count) String() string {
	return fmt.Sprintf("Count(%s)", c.Input.String())
}

// Difference represents an difference() function call.
type Difference struct {
	Inputs BitmapCalls
}

// String returns the string representation of the call.
func (c *Difference) String() string {
	return fmt.Sprintf("Difference(%s)", c.Inputs.String())
}

// Intersect represents an intersect() function call.
type Intersect struct {
	Inputs BitmapCalls
}

// String returns the string representation of the call.
func (c *Intersect) String() string {
	return fmt.Sprintf("Intersect(%s)", c.Inputs.String())
}

// Profile represents a Profile() function call.
type Profile struct {
	ID uint64
}

// String returns the string representation of the call.
func (c *Profile) String() string {
	args := make([]string, 0, 1)
	args = append(args, fmt.Sprintf("id=%d", c.ID))
	return fmt.Sprintf("Profile(%s)", strings.Join(args, ", "))
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
	return fmt.Sprintf("Range(%s)", strings.Join(args, ", "))
}

// SetBit represents a SetBit() function call.
type SetBit struct {
	ID        uint64
	Frame     string
	ProfileID uint64
	Timestamp *time.Time
}

// String returns the string representation of the call.
func (c *SetBit) String() string {
	args := make([]string, 0, 4)
	args = append(args, fmt.Sprintf("id=%d", c.ID))
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	if c.ProfileID != 0 {
		args = append(args, fmt.Sprintf("profileID=%d", c.ProfileID))
	}
	if c.Timestamp != nil {
		layout := "2006-01-02T15:04:05"
		args = append(args, fmt.Sprintf("timestamp=%s", c.Timestamp.Format(layout)))
	}
	return fmt.Sprintf("SetBit(%s)", strings.Join(args, ", "))
}

// SetBitmapAttrs represents a SetBitmapAttrs() function call.
type SetBitmapAttrs struct {
	ID    uint64
	Frame string
	Attrs map[string]interface{}
}

// String returns the string representation of the call.
func (c *SetBitmapAttrs) String() string {
	args := make([]string, 0, 2)
	args = append(args, fmt.Sprintf("id=%d", c.ID))
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}

	// Sort keys.
	keys := make([]string, 0, len(c.Attrs))
	for k := range c.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write key/value pairs.
	for _, k := range keys {
		if c.Attrs[k] == nil {
			args = append(args, fmt.Sprintf("%s=null", k))
			continue
		}

		switch v := c.Attrs[k].(type) {
		case string:
			args = append(args, fmt.Sprintf("%s=\"%s\"", k, v))
		default:
			args = append(args, fmt.Sprintf("%s=%v", k, v))
		}
	}

	return fmt.Sprintf("SetBitmapAttrs(%s)", strings.Join(args, ", "))
}

// SetProfileAttrs represents a SetProfileAttrs() function call.
type SetProfileAttrs struct {
	ID    uint64
	Attrs map[string]interface{}
}

// String returns the string representation of the call.
func (c *SetProfileAttrs) String() string {
	args := make([]string, 0, 2)
	args = append(args, fmt.Sprintf("id=%d", c.ID))

	// Sort keys.
	keys := make([]string, 0, len(c.Attrs))
	for k := range c.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write key/value pairs.
	for _, k := range keys {
		if c.Attrs[k] == nil {
			args = append(args, fmt.Sprintf("%s=null", k))
			continue
		}

		switch v := c.Attrs[k].(type) {
		case string:
			args = append(args, fmt.Sprintf("%s=\"%s\"", k, v))
		default:
			args = append(args, fmt.Sprintf("%s=%v", k, v))
		}
	}

	return fmt.Sprintf("SetProfileAttrs(%s)", strings.Join(args, ", "))
}

// TopN represents a TopN() function call.
type TopN struct {
	Frame string

	// Maximum number of results to return.
	N int

	// Bitmap to use for intersection while computing top results.
	// Original bitmap counts are used if no Src is provided.
	Src BitmapCall

	// Specific bitmaps to retrieve.
	BitmapIDs []uint64

	// Field name and values to filter on.
	Field   string
	Filters []interface{}
}

// String returns the string representation of the call.
func (c *TopN) String() string {
	args := make([]string, 0, 2)
	if c.Src != nil {
		args = append(args, c.Src.String())
	}
	if c.Frame != "" {
		args = append(args, fmt.Sprintf("frame=%s", c.Frame))
	}
	if c.N > 0 {
		args = append(args, fmt.Sprintf("n=%d", c.N))
	}
	if len(c.BitmapIDs) > 0 {
		strs := make([]string, len(c.BitmapIDs))
		for i := range c.BitmapIDs {
			strs[i] = strconv.FormatUint(c.BitmapIDs[i], 10)
		}
		args = append(args, fmt.Sprintf("ids=[%s]", strings.Join(strs, ",")))
	}
	if c.Field != "" {
		args = append(args, fmt.Sprintf("field=%q", c.Field))
	}
	if len(c.Filters) > 0 {
		filters := make([]string, 0, len(c.Filters))
		for i := range c.Filters {
			switch filter := c.Filters[i].(type) {
			case string:
				filters = append(filters, fmt.Sprintf("%q", filter))
			default:
				filters = append(filters, fmt.Sprintf("%v", filter))
			}
		}
		args = append(args, fmt.Sprintf("[%s]", strings.Join(filters, ",")))
	}
	return fmt.Sprintf("TopN(%s)", strings.Join(args, ", "))
}

// Union represents a union() function call.
type Union struct {
	Inputs BitmapCalls
}

// String returns the string representation of the call.
func (c *Union) String() string {
	return fmt.Sprintf("Union(%s)", c.Inputs.String())
}
