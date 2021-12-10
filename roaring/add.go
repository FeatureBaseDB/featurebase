package roaring

import (
	"fmt"
	"math/bits"
	"unsafe"
)

// Add two BSI bitmaps producing a new BSI bitmap.
func Add(x, y []*Bitmap) []*Bitmap {
	// Collect iterators.
	type itNode struct {
		it   ContainerIterator
		c    *Container
		key  uint64
		done bool
	}
	xits := make([]itNode, len(x))
	for i, b := range x {
		it, _ := b.Containers.Iterator(0)
		defer it.Close()

		node := &xits[i]

		node.it = it
		if !it.Next() {
			node.done = true
			continue
		}

		node.key, node.c = it.Value()
	}
	yits := make([]itNode, len(y))
	for i, b := range y {
		it, _ := b.Containers.Iterator(0)
		defer it.Close()

		node := &yits[i]

		node.it = it
		if !it.Next() {
			node.done = true
			continue
		}

		node.key, node.c = it.Value()
	}

	bits := len(x)
	if len(y) > len(x) {
		bits = len(y)
	}

	var carryRing [2]carryBuffer
	var temp [1024]uint64
	var dst []*Bitmap
	for {
		key := ^uint64(0)
		for i := range xits {
			if xits[i].done {
				continue
			}

			k := xits[i].key
			if k < key {
				key = k
			}
		}
		for i := range yits {
			if yits[i].done {
				continue
			}

			k := yits[i].key
			if k < key {
				key = k
			}
		}
		if key == ^uint64(0) {
			break
		}

		carryRing[0].clear()
		for i := 0; i <= bits; i++ {
			var x, y *Container
			if i < len(xits) && xits[i].key == key && !xits[i].done {
				x = xits[i].c
				if xits[i].it.Next() {
					xits[i].key, xits[i].c = xits[i].it.Value()
				} else {
					xits[i].done = true
				}
			}
			if i < len(yits) && yits[i].key == key && !yits[i].done {
				y = yits[i].c
				if yits[i].it.Next() {
					yits[i].key, yits[i].c = yits[i].it.Value()
				} else {
					yits[i].done = true
				}
			}

			c := fullAddContainers(x, y, &carryRing[i%2], &carryRing[1-(i%2)], &temp)
			if c == nil {
				continue
			}

			for i >= len(dst) {
				dst = append(dst, NewBitmap())
			}
			dst[i].Containers.Put(key, c)
		}
	}

	return dst
}

// fullAddContainers implements the bitwise formula for a 3-input-2-output full adder.
// Any of the 3 inputs may be nil, in which case they are treated as zeroes.
// The carry is written to carryOut, which must not be nil.
// The temp buffer will be used to store intermediate values, and can be safely stack-allocated.
func fullAddContainers(x, y *Container, carryIn, carryOut *carryBuffer, temp *[1024]uint64) *Container {
	if roaringParanoia {
		x.CheckN()
		y.CheckN()
		carryIn.check()
		defer carryOut.check()
	}

	// Accumulate inputs.
	var xm, ym, zm *[1024]uint64
	var xa, ya, za []uint16
	switch x.typ() {
	case ContainerNil:
	case ContainerBitmap:
		xm = x.bitmask()
	case ContainerArray:
		xa = x.array()
	case ContainerRun:
		// Create a temporary mask on the stack.
		// This should not happen very often.
		var mask [1024]uint64
		for _, r := range x.runs() {
			splatRun(&mask, r)
		}
		xm = &mask
	default:
		panic("invalid container type")
	}
	switch y.typ() {
	case ContainerNil:
	case ContainerBitmap:
		ym = y.bitmask()
	case ContainerArray:
		ya = y.array()
	case ContainerRun:
		// Create a temporary mask on the stack.
		// This should not happen very often.
		var mask [1024]uint64
		for _, r := range y.runs() {
			splatRun(&mask, r)
		}
		ym = &mask
	default:
		panic("invalid container type")
	}
	if carryIn != nil && carryIn.count > 0 {
		if carryIn.isBitmap {
			zm = carryIn.bitmap()
		} else {
			za = carryIn.array()[:carryIn.count]
		}
	}

	// Handle each of the 27 possible mask/array/nil combinations.
	switch {
	case xm != nil:
		dst, carry := temp, carryOut.bitmap()
		var do, co uint32
		switch {
		case ym != nil:
			switch {
			case zm != nil:
				do, co = addMaskMaskMaskToMask(dst, carry, xm, ym, zm)
			case len(za) > 0:
				do, co = addArrayMaskMaskToMask(dst, carry, za, xm, ym)
			default:
				do, co = addMaskMaskToMask(dst, carry, xm, ym)
			}
		case len(ya) > 0:
			switch {
			case zm != nil:
				do, co = addArrayMaskMaskToMask(dst, carry, ya, xm, zm)
			case len(za) > 0:
				do, co = addArrayArrayMaskToMask(dst, carry, ya, za, xm, uint32(x.N()))
			default:
				do, co = addArrayArrayMaskToMask(dst, carry, ya, nil, xm, uint32(x.N()))
			}
		default:
			switch {
			case zm != nil:
				do, co = addMaskMaskToMask(dst, carry, xm, zm)
			case len(za) > 0:
				do, co = addArrayArrayMaskToMask(dst, carry, za, nil, xm, uint32(x.N()))
			default:
				carryOut.clear()
				return x
			}
		}
		if co > 0 {
			carryOut.isBitmap = true
			carryOut.count = co
		} else {
			carryOut.clear()
		}
		return NewContainerBitmapN(append([]uint64(nil), dst[:]...), int32(do))
	case len(xa) > 0:
		switch {
		case ym != nil:
			dst, carry := temp, carryOut.bitmap()
			var do, co uint32
			switch {
			case zm != nil:
				do, co = addArrayMaskMaskToMask(dst, carry, xa, ym, zm)
			case len(za) > 0:
				do, co = addArrayArrayMaskToMask(dst, carry, xa, za, ym, uint32(y.N()))
			default:
				do, co = addArrayArrayMaskToMask(dst, carry, xa, nil, ym, uint32(y.N()))
			}
			if co > 0 {
				carryOut.isBitmap = true
				carryOut.count = co
			} else {
				carryOut.clear()
			}
			if do == 0 {
				return nil
			}
			return NewContainerBitmapN(append([]uint64(nil), dst[:]...), int32(do))
		case len(ya) > 0:
			switch {
			case zm != nil:
				do, co := addArrayArrayMaskToMask(temp, carryOut.bitmap(), xa, ya, zm, carryIn.count)
				if co > 0 {
					carryOut.isBitmap = true
					carryOut.count = co
				} else {
					carryOut.clear()
				}
				if do == 0 {
					return nil
				}
				return NewContainerBitmapN(append([]uint64(nil), temp[:]...), int32(do))
			case len(za) > 0:
				if do, co := addArrayArrayArrayToArray((*[4096]uint16)(unsafe.Pointer(temp)), carryOut.array(), xa, ya, za); do|co != ^uint16(0) {
					carryOut.isBitmap = false
					carryOut.count = uint32(co)
					if do == 0 {
						return nil
					}
					return NewContainerArrayCopy((*[4096]uint16)(unsafe.Pointer(temp))[:do])
				}

				do, co := addArrayArrayArrayToMask(temp, carryOut.bitmap(), xa, ya, za)
				if co > 0 {
					carryOut.isBitmap = true
					carryOut.count = co
				} else {
					carryOut.clear()
				}
				if do == 0 {
					return nil
				}
				return NewContainerBitmapN(append([]uint64(nil), temp[:]...), int32(do))
			default:
				if do, co := addArrayArrayToArray((*[4096]uint16)(unsafe.Pointer(temp)), carryOut.array(), xa, ya); do|co != ^uint16(0) {
					carryOut.isBitmap = false
					carryOut.count = uint32(co)
					if do == 0 {
						return nil
					}
					return NewContainerArrayCopy((*[4096]uint16)(unsafe.Pointer(temp))[:do])
				}

				do, co := addArrayArrayArrayToMask(temp, carryOut.bitmap(), xa, ya, nil)
				if co > 0 {
					carryOut.isBitmap = true
					carryOut.count = co
				} else {
					carryOut.clear()
				}
				if do == 0 {
					return nil
				}
				return NewContainerBitmapN(append([]uint64(nil), temp[:]...), int32(do))
			}
		default:
			switch {
			case zm != nil:
				do, co := addArrayArrayMaskToMask(temp, carryOut.bitmap(), xa, nil, zm, carryIn.count)
				if co > 0 {
					carryOut.isBitmap = true
					carryOut.count = co
				} else {
					carryOut.clear()
				}
				if do == 0 {
					return nil
				}
				return NewContainerBitmapN(append([]uint64(nil), temp[:]...), int32(do))
			case len(za) > 0:
				if do, co := addArrayArrayToArray((*[4096]uint16)(unsafe.Pointer(temp)), carryOut.array(), xa, za); do|co != ^uint16(0) {
					carryOut.isBitmap = false
					carryOut.count = uint32(co)
					if do == 0 {
						return nil
					}
					return NewContainerArrayCopy((*[4096]uint16)(unsafe.Pointer(temp))[:do])
				}

				do, co := addArrayArrayArrayToMask(temp, carryOut.bitmap(), xa, za, nil)
				if co > 0 {
					carryOut.isBitmap = true
					carryOut.count = co
				} else {
					carryOut.clear()
				}
				if do == 0 {
					return nil
				}
				return NewContainerBitmapN(append([]uint64(nil), temp[:]...), int32(do))
			default:
				carryOut.clear()
				return x
			}
		}
	default:
		switch {
		case ym != nil:
			dst, carry := temp, carryOut.bitmap()
			var do, co uint32
			switch {
			case zm != nil:
				do, co = addMaskMaskToMask(dst, carry, ym, zm)
			case len(za) > 0:
				do, co = addArrayArrayMaskToMask(dst, carry, za, nil, ym, uint32(y.N()))
			default:
				carryOut.clear()
				return y
			}
			if co > 0 {
				carryOut.isBitmap = true
				carryOut.count = co
			} else {
				carryOut.clear()
			}
			if do == 0 {
				return nil
			}
			return NewContainerBitmapN(append([]uint64(nil), dst[:]...), int32(do))
		case len(ya) > 0:
			switch {
			case zm != nil:
				do, co := addArrayArrayMaskToMask(temp, carryOut.bitmap(), ya, nil, zm, carryIn.count)
				if co > 0 {
					carryOut.isBitmap = true
					carryOut.count = co
				} else {
					carryOut.clear()
				}
				if do == 0 {
					return nil
				}
				return NewContainerBitmapN(append([]uint64(nil), temp[:]...), int32(do))
			case len(za) > 0:
				if do, co := addArrayArrayToArray((*[4096]uint16)(unsafe.Pointer(temp)), carryOut.array(), ya, za); do|co != ^uint16(0) {
					carryOut.isBitmap = false
					carryOut.count = uint32(co)
					if do == 0 {
						return nil
					}
					return NewContainerArrayCopy((*[4096]uint16)(unsafe.Pointer(temp))[:do])
				}

				do, co := addArrayArrayArrayToMask(temp, carryOut.bitmap(), ya, za, nil)
				if co > 0 {
					carryOut.isBitmap = true
					carryOut.count = co
				} else {
					carryOut.clear()
				}
				if do == 0 {
					return nil
				}
				return NewContainerBitmapN(append([]uint64(nil), temp[:]...), int32(do))
			default:
				carryOut.clear()
				return y
			}
		default:
			carryOut.clear()
			return carryIn.containerize()
		}
	}
}

// carryBuffer is a buffer used to store carry bits.
// It is designed to be stack-allocated.
// As such **IT SHOULD NOT BE POOLED**.
type carryBuffer struct {
	isBitmap bool
	count    uint32
	data     [1024]uint64
}

func (b *carryBuffer) bitmap() *[1024]uint64 {
	return &b.data
}

func (b *carryBuffer) array() *[4096]uint16 {
	return (*[4096]uint16)(unsafe.Pointer(&b.data))
}

// compact converts the buffer to an array if it would be more efficient.
func (b *carryBuffer) compact() {
	if b.isBitmap && b.count < 4096 {
		b.compactSlow()
	}
}

func (b *carryBuffer) compactSlow() {
	var buf [4096]uint16
	i := 0
	for j, v := range b.data {
		for v != 0 {
			k := bits.TrailingZeros64(v)
			v &^= 1 << k
			buf[i] = 64*uint16(j) + uint16(k)
			i++
		}
	}
	copy(b.array()[:], buf[:i])
	b.isBitmap = false
	if roaringParanoia {
		b.check()
	}
}

// clear the buffer, making it effectively full of zeroes.
func (b *carryBuffer) clear() {
	b.isBitmap = false
	b.count = 0
}

// check that invariants hold.
// This exists mainly for debugging.
func (b *carryBuffer) check() {
	if b.isBitmap {
		var count int
		for _, v := range b.bitmap() {
			count += bits.OnesCount64(v)
		}
		if uint32(count) != b.count {
			panic(fmt.Errorf("count mismatch: reported %d but got %d", b.count, count))
		}
	} else {
		if b.count > uint32(len(b.array())) {
			panic(fmt.Errorf("found too many bits: %d of a max of %d", b.count, len(b.array())))
		}
		if b.count > 0 {
			arr := b.array()[:b.count]
			for i, v := range arr {
				if i > 0 && v <= arr[i-1] {
					panic(fmt.Errorf("broken array: %d after %d", v, arr[i-1]))
				}
			}
		}
	}
}

// containerize the contents of the buffer.
func (b *carryBuffer) containerize() *Container {
	if b.count == 0 {
		return nil
	}

	b.compact()

	if !b.isBitmap {
		return NewContainerArray(append([]uint16(nil), b.array()[:b.count]...))
	}

	return NewContainerBitmapN(append([]uint64(nil), b.bitmap()[:]...), int32(b.count))
}

// addArrayArrayToArray implemnents a half-adder over two arrays, producing array outputs.
// If the results are too big, this returns ^uint16(0) to indicate that the operation failed.
func addArrayArrayToArray(dst, carry *[4096]uint16, x, y []uint16) (uint16, uint16) {
	_, _ = &dst[0], &carry[0]

	// Half-add x and y.
	i, j, do, co := 0, 0, 0, 0
	for i < len(x) && j < len(y) {
		a, b := x[i], y[j]
		switch {
		case a < b:
			// Copy all values under b to the lower output bit.
			for ; i < len(x) && x[i] < b; i++ {
				if do >= len(dst) {
					return ^uint16(0), ^uint16(0)
				}

				dst[do] = x[i]
				do++
			}
		case b < a:
			// Copy all values under a to the lower output bit.
			for ; j < len(y) && y[j] < a; j++ {
				if do >= len(dst) {
					return ^uint16(0), ^uint16(0)
				}

				dst[do] = y[j]
				do++
			}
		default:
			// Copy the value to the carry.
			if co >= len(carry) {
				return ^uint16(0), ^uint16(0)
			}
			carry[co] = a
			co++
			i++
			j++
		}
	}

	// Copy the remaining data to the lower output bit.
	var remaining []uint16
	switch {
	case i < len(x):
		remaining = x[i:]
	case j < len(y):
		remaining = y[j:]
	}
	if do+len(remaining) > len(dst) {
		return ^uint16(0), ^uint16(0)
	}
	copy(dst[do:], remaining)
	return uint16(do + len(remaining)), uint16(co)
}

// addArrayArrayArrayToArray implemnents a full-adder over three arrays, producing array outputs.
// If the results are too big, this returns ^uint16(0) to indicate that the operation failed.
func addArrayArrayArrayToArray(dst, carry *[4096]uint16, x, y, z []uint16) (uint16, uint16) {
	_, _ = &dst[0], &carry[0]

	// Run a full adder.
	i, j, k, do, co := 0, 0, 0, 0, 0
	for i < len(x) && j < len(y) && k < len(z) {
		a, b, c := x[i], y[j], z[k]
		switch {
		case a < b && a < c:
			// Find the lowest value in the other two inputs.
			next := b
			if c < b {
				next = c
			}

			// Copy every value below that to the lower output bit.
			for ; i < len(x) && x[i] < next; i++ {
				if do >= len(dst) {
					return ^uint16(0), ^uint16(0)
				}

				dst[do] = x[i]
				do++
			}
		case b < a && b < c:
			// Find the lowest value in the other two inputs.
			next := a
			if c < a {
				next = c
			}

			// Copy every value below that to the lower output bit.
			for ; j < len(y) && y[j] < next; j++ {
				if do >= len(dst) {
					return ^uint16(0), ^uint16(0)
				}

				dst[do] = y[j]
				do++
			}
		case c < a && c < b:
			// Find the lowest value in the other two inputs.
			next := a
			if b < a {
				next = b
			}

			// Copy every value below that to the lower output bit.
			for ; k < len(z) && z[k] < next; k++ {
				if do >= len(dst) {
					return ^uint16(0), ^uint16(0)
				}

				dst[do] = z[k]
				do++
			}

		case co >= len(carry):
			// At least two bits are set, so a carry bit will be produced.
			return ^uint16(0), ^uint16(0)
		case a == b && b != c:
			// Carry the lower value (a/b).
			carry[co] = a
			co++
			i++
			j++
		case b == c && a != b:
			// Carry the lower value (b/c).
			carry[co] = b
			co++
			j++
			k++
		case a == c && a != b:
			// Carry the lower value (a/c).
			carry[co] = a
			co++
			i++
			k++

		case do >= len(dst):
			// All three inputs are set, so this will produce both a lower bit and a carry.
			return ^uint16(0), ^uint16(0)
		default:
			// a == b == c; 1+1+1 = 0b11
			dst[do] = a
			do++
			carry[co] = a
			co++
			i++
			j++
			k++
		}
	}

	// Run a half adder.
	if k < len(z) {
		// Re-order the inputs such that the inputs (if any) which still have data are x and y.
		if i < len(x) {
			j, y = k, z
		} else {
			i, x = k, z
		}
	}
	for i < len(x) && j < len(y) {
		a, b := x[i], y[j]
		switch {
		case a < b:
			// Copy all values under b to the lower output bit.
			for ; i < len(x) && x[i] < b; i++ {
				if do >= len(dst) {
					return ^uint16(0), ^uint16(0)
				}

				dst[do] = x[i]
				do++
			}
		case b < a:
			// Copy all values under a to the lower output bit.
			for ; j < len(y) && y[j] < a; j++ {
				if do >= len(dst) {
					return ^uint16(0), ^uint16(0)
				}

				dst[do] = y[j]
				do++
			}
		default:
			// Copy the value to the carry.
			if co >= len(carry) {
				return ^uint16(0), ^uint16(0)
			}
			carry[co] = a
			co++
			i++
			j++
		}
	}

	// Copy the remaining data to the lower output bit.
	var remaining []uint16
	switch {
	case i < len(x):
		remaining = x[i:]
	case j < len(y):
		remaining = y[j:]
	}
	if do+len(remaining) > len(dst) {
		return ^uint16(0), ^uint16(0)
	}
	copy(dst[do:], remaining)
	return uint16(do + len(remaining)), uint16(co)
}

// addArrayArrayArrayToMask implemnents a full-adder over three arrays, producing bitmask outputs.
// This is generally only needed when addArrayArrayArrayToArray fails.
func addArrayArrayArrayToMask(dst, carry *[1024]uint64, x, y, z []uint16) (uint32, uint32) {
	// Start with blank outputs.
	*dst = [1024]uint64{}
	*carry = [1024]uint64{}

	var co uint64
	for _, v := range x {
		// Add each value to the lower output bit.
		dst[v/64] |= 1 << (v % 64)

		// There is no carry because this is the first copy of the output.
	}
	for _, v := range y {
		// Increment the carry output counter if the value is already included.
		co += (dst[v/64] >> (v % 64)) & 1

		// Insert the carry bit if the value is already in the lower output bit.
		carry[v/64] |= dst[v/64] & (1 << (v % 64))

		// Flip the lower output bit for the value.
		dst[v/64] ^= 1 << (v % 64)
	}
	for _, v := range z {
		// Increment the carry output counter if the value is already included.
		co += (dst[v/64] >> (v % 64)) & 1

		// Insert the carry bit if the value is already in the lower output bit.
		carry[v/64] |= dst[v/64] & (1 << (v % 64))

		// Flip the lower output bit for the value.
		dst[v/64] ^= 1 << (v % 64)
	}

	// If there were no collisions, the number of values in the lower output bit would be equal to the sum of the counts of the inputs.
	// For each carry, we subtract 2 as it was produced by combining two copies of a value.
	return uint32(len(x)+len(y)+len(z)) - 2*uint32(co), uint32(co)
}

// addArrayArrayMaskToMask implemnents a full-adder over two arrays and one bitmask, producing bitmask outputs.
func addArrayArrayMaskToMask(dst, carry *[1024]uint64, x, y []uint16, z *[1024]uint64, zc uint32) (uint32, uint32) {
	// Clear the carry output.
	*carry = [1024]uint64{}

	// Copy the mask input to the lower output bit.
	*dst = *z

	var co uint64
	for _, v := range x {
		// Increment the carry output counter if the value is already included.
		co += (dst[v/64] >> (v % 64)) & 1

		// Insert the carry bit if the value is already in the lower output bit.
		carry[v/64] |= dst[v/64] & (1 << (v % 64))

		// Flip the lower output bit for the value.
		dst[v/64] ^= 1 << (v % 64)
	}
	for _, v := range y {
		// Increment the carry output counter if the value is already included.
		co += (dst[v/64] >> (v % 64)) & 1

		// Insert the carry bit if the value is already in the lower output bit.
		carry[v/64] |= dst[v/64] & (1 << (v % 64))

		// Flip the lower output bit for the value.
		dst[v/64] ^= 1 << (v % 64)
	}

	// If there were no collisions, the number of values in the lower output bit would be equal to the sum of the counts of the inputs.
	// For each carry, we subtract 2 as it was produced by combining two copies of a value.
	return uint32(len(x)+len(y)) + zc - 2*uint32(co), uint32(co)
}

// addArrayArrayMaskToMask implemnents a full-adder over one array and two bitmasks, producing bitmask outputs.
func addArrayMaskMaskToMask(dst, carry *[1024]uint64, x []uint16, y, z *[1024]uint64) (uint32, uint32) {
	_, _, _, _ = &dst[0], &carry[0], &y[0], &z[0]

	// Do a bitwise combine of y and z into dst and carry.
	var doi, coi int
	for i := range dst {
		yv, zv := y[i], z[i]
		dstv, carryv := yv^zv, yv&zv
		dst[i], carry[i] = dstv, carryv
		doi += bits.OnesCount64(dstv)
		coi += bits.OnesCount64(carryv)
	}

	// Add the array values.
	var newco uint64
	for _, v := range x {
		// Increment the carry output counter if the value is already included.
		newco += (dst[v/64] >> (v % 64)) & 1

		// Insert the carry bit if the value is already in the lower output bit.
		carry[v/64] |= dst[v/64] & (1 << (v % 64))

		// Flip the lower output bit for the value.
		dst[v/64] ^= 1 << (v % 64)
	}

	// If there were no collisions, the number of values in the lower output bit would be equal to the sum of the counts of the inputs.
	// For each carry, we subtract 2 as it was produced by combining two copies of a value.
	return uint32(doi) + uint32(len(x)) - 2*uint32(newco), uint32(coi) + uint32(newco)
}

// addMaskMaskToMask implemnents a half-adder over two bitmasks, producing bitmask outputs.
func addMaskMaskToMask(dst, carry *[1024]uint64, x, y *[1024]uint64) (uint32, uint32) {
	_, _, _, _ = &dst[0], &carry[0], &x[0], &y[0]

	// Do a bitwise combine of both inputs into dst and carry.
	var do, co int
	for i := range dst {
		xv, yv := x[i], y[i]
		dstv, carryv := xv^yv, xv&yv
		dst[i], carry[i] = dstv, carryv
		do += bits.OnesCount64(dstv)
		co += bits.OnesCount64(carryv)
	}

	return uint32(do), uint32(co)
}

// addMaskMaskMaskToMask implemnents a full-adder over three bitmasks, producing bitmask outputs.
func addMaskMaskMaskToMask(dst, carry *[1024]uint64, x, y, z *[1024]uint64) (uint32, uint32) {
	_, _, _, _, _ = &dst[0], &carry[0], &x[0], &y[0], &z[0]

	// Do a bitwise combine of all three inputs into dst and carry.
	var do, co int
	for i := range dst {
		xv, yv, zv := x[i], y[i], z[i]
		dstv, carryv := xv^yv^zv, (xv&yv)|(yv&zv)|(xv&zv)
		dst[i], carry[i] = dstv, carryv
		do += bits.OnesCount64(dstv)
		co += bits.OnesCount64(carryv)
	}

	return uint32(do), uint32(co)
}
