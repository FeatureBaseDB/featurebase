// Copyright 2019 Pilosa Corp.
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

// Package ext provides an EXPERIMENTAL AND TEMPORARY interface to use for
// plugin extensions to Pilosa. DO NOT DEVELOP NEW PLUGINS WITH THIS. The
// replacement design is already in process, but it needs more refinement
// to address issues. This one has those issues, and more.
//
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

import "sync"

// The Bitmap type represents a Pilosa bitmap, and is used for bitmap
// operations.
type Bitmap interface {
	// AddN and RemoveN can be used to add or remove values from a bitmap.
	AddN(a ...uint64) (int, error)
	RemoveN(a ...uint64) (int, error)

	// Lookups
	Max() uint64
	Min() (uint64, bool)
	Count() uint64
	Any() bool
	Contains(uint64) bool
	Slice() []uint64
	SliceRange(uint64, uint64) []uint64
	// ContainerBits stores the next 1<<16 bits, starting at the provided
	// bit index. It may use a provided []uint64 to store them, or may
	// provide its own. Don't write to those bits. Offset must be a multiple
	// of 1<<16.
	ContainerBits(uint64, []uint64) []uint64

	// These operators provide existing implemented binary ops.
	Intersect(Bitmap) Bitmap
	Union(Bitmap) Bitmap
	IntersectionCount(Bitmap) uint64
	Difference(Bitmap) Bitmap
	Xor(Bitmap) Bitmap
	Shift(int) (Bitmap, error)
	Flip(uint64, uint64) Bitmap

	// New() is an atrocity: it creates a new bitmap, unrelated to the
	// existing bitmap. This lets you create a new bitmap without having
	// imported any of the packages that have bitmap creation tools, because
	// the bitmap wrapper type has to give you one.
	New() Bitmap
}

// SignedBitmap represents a bitmap that can contain both positive and negative
// values.
type SignedBitmap struct {
	Pos, Neg Bitmap
}

// A BitmapOp represents a new bitmap operation that should be exposed
// in PQL.

type BitmapOpInput byte
type BitmapOpOutput byte
type BitmapOpArity byte
type BitmapOpPrecall byte
type BitmapOpType struct {
	Input   BitmapOpInput
	Arity   BitmapOpArity
	Output  BitmapOpOutput
	Precall BitmapOpPrecall
}

const (
	OpArityUnary = BitmapOpArity(iota)
	OpArityBinary
	OpArityNary
)

const (
	// Unary: Exactly one bitmap.
	OpInputBitmap = BitmapOpInput(iota)
	// The really weird special case used for BSI, where we end up
	// needing to do BSI computations. Arguments will be a
	// single BitmapBSI, and a []Bitmap for other operands if any.
	OpInputNaryBSI
)

const (
	OpOutputCount = BitmapOpOutput(iota)
	OpOutputBitmap
	OpOutputSignedBitmap
)

const (
	OpPrecallNone = BitmapOpPrecall(iota)
	OpPrecallGlobal
	OpPrecallLocal // unimplemented
)

// Regardless of arity, non-BSI functions should always take []Bitmap.
type BitmapOpFunc interface {
	BitmapOpType() BitmapOpType
}

// BitmapOpBitmap should actually always be func([]Bitmap) Bitmap, but
// might be different kinds.
type BitmapOpBitmap interface {
	BitmapOpArity() BitmapOpArity
	BitmapOpFunc() GenericBitmapOpBitmap
}

// the common underlying type of the other BitmapOpBitmap functions
type GenericBitmapOpBitmap func([]Bitmap, map[string]interface{}) Bitmap

// BitmapBSI represents the way a single BSI field is passed into a function
// which takes a BSI field.
type BitmapBSI struct {
	FieldData  Bitmap
	ShardWidth uint64
	Offset     int64
	Depth      uint
}

type BitmapOpBSIBitmap func(BitmapBSI, []Bitmap, map[string]interface{}) SignedBitmap

func (b BitmapOpBSIBitmap) BitmapOpType() BitmapOpType {
	return BitmapOpType{Input: OpInputNaryBSI, Arity: OpArityNary, Output: OpOutputSignedBitmap}
}

type BitmapOpBSIBitmapPrecall func(BitmapBSI, []Bitmap, map[string]interface{}) SignedBitmap

func (b BitmapOpBSIBitmapPrecall) BitmapOpType() BitmapOpType {
	return BitmapOpType{Input: OpInputNaryBSI, Arity: OpArityNary, Precall: OpPrecallGlobal, Output: OpOutputSignedBitmap}
}

type BitmapOpUnaryCount func([]Bitmap, map[string]interface{}) int64

func (b BitmapOpUnaryCount) BitmapOpType() BitmapOpType {
	return BitmapOpType{Input: OpInputBitmap, Arity: OpArityUnary, Output: OpOutputCount}
}

type BitmapOpUnaryBitmap func([]Bitmap, map[string]interface{}) Bitmap

func (b BitmapOpUnaryBitmap) BitmapOpType() BitmapOpType {
	return BitmapOpType{Input: OpInputBitmap, Arity: OpArityUnary, Output: OpOutputBitmap}
}

func (b BitmapOpUnaryBitmap) BitmapOpArity() BitmapOpArity {
	return OpArityUnary
}

func (b BitmapOpUnaryBitmap) BitmapOpFunc() GenericBitmapOpBitmap {
	return GenericBitmapOpBitmap(b)
}

type BitmapOpBinaryBitmap func([]Bitmap, map[string]interface{}) Bitmap

func (b BitmapOpBinaryBitmap) BitmapOpType() BitmapOpType {
	return BitmapOpType{Input: OpInputBitmap, Arity: OpArityBinary, Output: OpOutputBitmap}
}

func (b BitmapOpBinaryBitmap) BitmapOpArity() BitmapOpArity {
	return OpArityBinary
}

func (b BitmapOpBinaryBitmap) BitmapOpFunc() GenericBitmapOpBitmap {
	return GenericBitmapOpBitmap(b)
}

type BitmapOpNaryBitmap func([]Bitmap, map[string]interface{}) Bitmap

func (b BitmapOpNaryBitmap) BitmapOpType() BitmapOpType {
	return BitmapOpType{Input: OpInputBitmap, Arity: OpArityNary, Output: OpOutputBitmap}
}

func (b BitmapOpNaryBitmap) BitmapOpArity() BitmapOpArity {
	return OpArityNary
}

func (b BitmapOpNaryBitmap) BitmapOpFunc() GenericBitmapOpBitmap {
	return GenericBitmapOpBitmap(b)
}

// BitmapOp represents an operation to be supported in PQL. Operations
// on bitmaps should always take []Bitmap. Operations on InputNaryBSI should
// take a []Bitmap, plus a Bitmap/shard-width/offset/depth.
//
// Reserved is a list of words to treat as reserved words in a prototype.
// This is not currently used but might be later, and I want to have the
// concept handy now.
type BitmapOp struct {
	Name     string
	Func     BitmapOpFunc
	Reserved []string
}

// ExtensionInfo tells us about the extension. The ExtensionAPI string
// should be "v0". The version is a human-readable version, use something
// that seems meaningful. Name and Description are reasonably self-explanatory,
// I hope.
//
// Extensions should define a function:
//   func ExtensionInfo(extensionAPI string) (*ExtensionInfo, error)
// which reports their extension info if they think they can coexist with that
// API string.
type ExtensionInfo struct {
	Name         string     // Extension name.
	Description  string     // Short description.
	Version      string     // Human-readable version info for extension.
	ExtensionAPI string     // Extension API version. Should be v0 for now.
	License      string     // License info.
	BitmapOps    []BitmapOp // List of provided ops.
}

var extMu sync.Mutex

var knownExtensions []*ExtensionInfo
var newExtensions []*ExtensionInfo

// RegisterExtension tells the extension system about a new extension.
func RegisterExtension(ext *ExtensionInfo) {
	extMu.Lock()
	defer extMu.Unlock()
	newExtensions = append(newExtensions, ext)
}

// NewExtentsions returns extensions that have been registered, but not previously
// returned by Newextensions.
func NewExtensions() []*ExtensionInfo {
	extMu.Lock()
	defer extMu.Unlock()
	knownExtensions = append(knownExtensions, newExtensions...)
	ret := newExtensions
	newExtensions = nil
	return ret
}
