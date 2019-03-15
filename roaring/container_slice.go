package roaring

import (
	"unsafe"
)

// Container represents a Container for uint16 integers.
//
// These are used for storing the low bits of numbers in larger sets of uint64.
// The high bits are stored in a Container's key which is tracked by a separate
// data structure. Integers in a Container can be encoded in one of three ways -
// the encoding used is usually whichever is most compact, though any Container
// type should be able to encode any set of integers safely. For containers with
// less than 4,096 values, an array is often used. Containers with long runs of
// integers would use run length encoding, and more random data usually uses
// bitmap encoding.
type Container struct {
	oneSlice []uint16 // and here is where the magic happens
	n        int32    // number of integers in container
	mapped   bool     // mapped directly to a byte slice when true
	typ      byte     // array, bitmap, or run
}

// NewContainer returns a new instance of container. This trivial function
// may later become more interesting.
func NewContainer() *Container {
	statsHit("NewContainer")
	return &Container{typ: containerArray}
}

// NewContainerBitmap makes a bitmap container using the provided bitmap, or
// an empty one if provided bitmap is nil. If the provided bitmap is too short,
// it will be padded.
func NewContainerBitmap(n int32, bitmap []uint64) *Container {
	if bitmap == nil {
		bitmap = make([]uint64, bitmapN)
	}
	// pad to required length
	if len(bitmap) < bitmapN {
		bm2 := make([]uint64, bitmapN)
		copy(bm2, bitmap)
		bitmap = bm2
	}
	return &Container{typ: containerBitmap, n: n, oneSlice: *(*[]uint16)(unsafe.Pointer(&bitmap))}
}

// NewContainerArray returns an array using the provided set of values. It's
// okay if the slice is nil; that's a length of zero.
func NewContainerArray(set []uint16) *Container {
	return &Container{typ: containerArray, n: int32(len(set)), oneSlice: *(*[]uint16)(unsafe.Pointer(&set))}

}

// NewContainerRun creates a new run array using a provided (possibly nil)
// slice of intervals.
func NewContainerRun(set []interval16) *Container {
	c := &Container{typ: containerRun, oneSlice: *(*[]uint16)(unsafe.Pointer(&set))}
	for _, run := range set {
		c.n += int32(run.last-run.start) + 1
	}
	return c
}

// array yields the data viewed as a slice of intervals.
func (c *Container) array() []uint16 {
	if roaringParanoia {
		if c.typ != containerArray {
			panic("attempt to read non-array's array")
		}
	}
	return *(*[]uint16)(unsafe.Pointer(&c.oneSlice))
}

// setArray stores a set of uint16s as data.
func (c *Container) setArray(array []uint16) {
	if roaringParanoia {
		if c.typ != containerArray {
			panic("attempt to write non-array's array")
		}
	}
	c.oneSlice = *(*[]uint16)(unsafe.Pointer(&array))
}

// bitmap yields the data viewed as a slice of uint64s holding bits.
func (c *Container) bitmap() []uint64 {
	if roaringParanoia {
		if c.typ != containerBitmap {
			panic("attempt to read non-bitmap's bitmap")
		}
	}
	return *(*[]uint64)(unsafe.Pointer(&c.oneSlice))
}

// setBitmap stores a set of uint64s as data.
func (c *Container) setBitmap(bitmap []uint64) {
	if roaringParanoia {
		if c.typ != containerBitmap {
			panic("attempt to write non-bitmap's bitmap")
		}
	}
	c.oneSlice = *(*[]uint16)(unsafe.Pointer(&bitmap))
}

// runs yields the data viewed as a slice of intervals.
func (c *Container) runs() []interval16 {
	if roaringParanoia {
		if c.typ != containerRun {
			panic("attempt to read non-run's runs")
		}
	}
	return *(*[]interval16)(unsafe.Pointer(&c.oneSlice))
}

// setRuns stores a set of intervals as data.
func (c *Container) setRuns(runs []interval16) {
	if roaringParanoia {
		if c.typ != containerRun {
			panic("attempt to write non-run's runs")
		}
	}
	c.oneSlice = *(*[]uint16)(unsafe.Pointer(&runs))
}

// Mapped returns true if the container is mapped directly to a byte slice
func (c *Container) Mapped() bool {
	return c.mapped
}

// N returns the cached bit count of the container
func (c *Container) N() int32 {
	return c.n
}

// Update updates the container
func (c *Container) Update(typ byte, n int32, mapped bool) {
	c.typ = typ
	c.n = n
	c.mapped = mapped
	// we don't know that any existing slice is usable, so let's ditch it
	c.oneSlice = nil
}

// isArray returns true if the container is an array container.
func (c *Container) isArray() bool {
	return c.typ == containerArray
}

// isBitmap returns true if the container is a bitmap container.
func (c *Container) isBitmap() bool {
	return c.typ == containerBitmap
}

// isRun returns true if the container is a run-length-encoded container.
func (c *Container) isRun() bool {
	return c.typ == containerRun
}

// unmap creates copies of the containers data in the heap.
//
// This is performed when altering the container since its contents could be
// pointing at a read-only mmap.
func (c *Container) unmap() {
	if !c.mapped {
		return
	}

	switch c.typ {
	case containerArray:
		tmp := make([]uint16, len(c.array()))
		copy(tmp, c.array())
		c.setArray(tmp)
	case containerBitmap:
		tmp := make([]uint64, len(c.bitmap()))
		copy(tmp, c.bitmap())
		c.setBitmap(tmp)
	case containerRun:
		tmp := make([]interval16, len(c.runs()))
		copy(tmp, c.runs())
		c.setRuns(tmp)
	}
	c.mapped = false
}
