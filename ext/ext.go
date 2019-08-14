// Package ext provides an interface to use for plugin extensions to Pilosa.
// In the current design, plugins will be loaded at runtime using the
// go `plugin` package, so they should be built as a main package using
// the plugin build mode.
//
// Plugins should not import other packages from Pilosa.
//
// To advertise their functionality, plugins define one or more of a
// handful of symbols which will be checked for at plugin load and used
// to register their functionality.
//
// The plugin interface will check for the following function(s). If the
// functions exist, they must have the given signatures. If they return
// a non-nil error, no ops are registered, and the error message will
// be reported in the Pilosa server's logs.
//
// BitmapOps() ([]BitmapOp, error)
//
// These functions may be absent, and may return nil slices; in either
// case, no ops are registered.
package ext

// The Bitmap type represents a Pilosa bitmap, and is used for bitmap
// operations.
type Bitmap interface {
	// Clone duplicates a bitmap. Use this when performing mutations
	// on operands.

	// AddN and RemoveN can be used to add or remove values from a bitmap.
	AddN(a ...uint64) (int, error)
	RemoveN(a ...uint64) (int, error)

	// Lookups
	Max() uint64
	Count() uint64
	Any() bool
	Contains(uint64) bool
	Slice() []uint64
	SliceRange(uint64, uint64) []uint64

	// These operators provide existing implemented binary ops.
	Intersect(Bitmap) Bitmap
	Union(Bitmap) Bitmap
	IntersectionCount(Bitmap) uint64
	Difference(Bitmap) Bitmap
	Xor(Bitmap) Bitmap
	Shift(int) (Bitmap, error)
	Flip(uint64, uint64) Bitmap
}

// A BitmapOp represents a new bitmap operation that should be exposed
// in PQL.

type BitmapOpType byte

const (
	OpUnary = BitmapOpType(iota)
	OpBinary
	OpNary
)

// BitmapOp represents an operation on one or more bitmaps, yielding
// a bitmap or count, to be supported in PQL. Only one of BitmapFunc or
// CountFunc should be present.
type BitmapOp struct {
	Name       string
	Arity      BitmapOpType
	BitmapFunc func([]Bitmap) Bitmap
	CountFunc  func([]Bitmap) uint64
}
