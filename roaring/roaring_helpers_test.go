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

package roaring

///////////////////////////////////////////////////////////////////////////

var containerWidth uint64 = 65536

////////////////// array
func arrayEmpty() []uint16 {
	return make([]uint16, 0)
}

func arrayFull() []uint16 {
	array := make([]uint16, containerWidth)
	for i := 0; i < int(containerWidth); i++ {
		array[i] = uint16(i)
	}
	return array
}

func arrayFirstBitSet() []uint16 {
	array := make([]uint16, 0)
	array = append(array, uint16(0))
	return array
}

func arrayLastBitSet() []uint16 {
	array := make([]uint16, 0)
	array = append(array, uint16(65535))
	return array
}

func arrayFirstBitUnset() []uint16 {
	array := make([]uint16, containerWidth-1)
	for i := 1; i < int(containerWidth); i++ {
		array[i-1] = uint16(i)
	}
	return array
}

func arrayLastBitUnset() []uint16 {
	array := make([]uint16, containerWidth-1)
	for i := 0; i < int(containerWidth)-1; i++ {
		array[i] = uint16(i)
	}
	return array
}

func arrayInnerBitsSet() []uint16 {
	array := make([]uint16, containerWidth-2)
	for i := 1; i < int(containerWidth)-1; i++ {
		array[i-1] = uint16(i)
	}
	return array
}

func arrayOuterBitsSet() []uint16 {
	array := make([]uint16, 0)
	array = append(array, uint16(0))
	array = append(array, uint16(65535))
	return array
}

////////////////// bitmap
func bitmapEmpty() []uint64 {
	return make([]uint64, bitmapN)
}

func bitmapFull() []uint64 {
	bitmap := make([]uint64, bitmapN)
	for i := 0; i < bitmapN; i++ {
		bitmap[i] = 0xFFFFFFFFFFFFFFFF
	}
	return bitmap
}

func bitmapFirstBitSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[0] = 0x0000000000000001
	return bitmap
}

func bitmapLastBitSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[bitmapN-1] = 0x8000000000000000
	return bitmap
}

func bitmapFirstBitUnset() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[0] = 0xFFFFFFFFFFFFFFFE
	for i := 1; i < bitmapN; i++ {
		bitmap[i] = 0xFFFFFFFFFFFFFFFF
	}
	return bitmap
}

func bitmapLastBitUnset() []uint64 {
	bitmap := make([]uint64, bitmapN)
	for i := 0; i < bitmapN-1; i++ {
		bitmap[i] = 0xFFFFFFFFFFFFFFFF
	}
	bitmap[bitmapN-1] = 0x7FFFFFFFFFFFFFFF
	return bitmap
}

func bitmapInnerBitsSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[0] = 0xFFFFFFFFFFFFFFFE
	for i := 1; i < bitmapN-1; i++ {
		bitmap[i] = 0xFFFFFFFFFFFFFFFF
	}
	bitmap[bitmapN-1] = 0x7FFFFFFFFFFFFFFF
	return bitmap
}

func bitmapOuterBitsSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[0] = 0x0000000000000001
	for i := 1; i < bitmapN-1; i++ {
		bitmap[i] = 0x0000000000000000
	}
	bitmap[bitmapN-1] = 0x8000000000000000
	return bitmap
}

////////////////// run
func runEmpty() []interval16 {
	return make([]interval16, 0)
}

func runFull() []interval16 {
	run := make([]interval16, 0)
	run = append(run, interval16{start: 0, last: 65535})
	return run
}

func runFirstBitSet() []interval16 {
	run := make([]interval16, 0)
	run = append(run, interval16{start: 0, last: 0})
	return run
}

func runLastBitSet() []interval16 {
	run := make([]interval16, 0)
	run = append(run, interval16{start: 65535, last: 65535})
	return run
}

func runFirstBitUnset() []interval16 {
	run := make([]interval16, 0)
	run = append(run, interval16{start: 1, last: 65535})
	return run
}

func runLastBitUnset() []interval16 {
	run := make([]interval16, 0)
	run = append(run, interval16{start: 0, last: 65534})
	return run
}

func runInnerBitsSet() []interval16 {
	run := make([]interval16, 0)
	run = append(run, interval16{start: 1, last: 65534})
	return run
}

func runOuterBitsSet() []interval16 {
	run := make([]interval16, 0)
	run = append(run, interval16{start: 0, last: 0})
	run = append(run, interval16{start: 65535, last: 65535})
	return run
}

///////////////////////////////////////////////////////////////////////////

type testOp struct {
	f   func(a, b *container) *container
	x   string
	y   string
	exp string
}

func doContainer(containerType byte, data interface{}) *container {
	c := &container{
		containerType: containerType,
	}

	switch containerType {
	case ContainerArray:
		c.array = data.([]uint16)
	case ContainerBitmap:
		c.bitmap = data.([]uint64)
	case ContainerRun:
		c.runs = data.([]interval16)
	}
	c.n = c.count()

	return c
}

func setupContainerTests() map[byte]map[string]*container {

	cts := make(map[byte]map[string]*container)

	// array containers
	cts[ContainerArray] = map[string]*container{
		"empty":         doContainer(ContainerArray, arrayEmpty()),
		"full":          doContainer(ContainerArray, arrayFull()),
		"firstBitSet":   doContainer(ContainerArray, arrayFirstBitSet()),
		"lastBitSet":    doContainer(ContainerArray, arrayLastBitSet()),
		"firstBitUnset": doContainer(ContainerArray, arrayFirstBitUnset()),
		"lastBitUnset":  doContainer(ContainerArray, arrayLastBitUnset()),
		"innerBitsSet":  doContainer(ContainerArray, arrayInnerBitsSet()),
		"outerBitsSet":  doContainer(ContainerArray, arrayOuterBitsSet()),
	}

	// bitmap containers
	cts[ContainerBitmap] = map[string]*container{
		"empty":         doContainer(ContainerBitmap, bitmapEmpty()),
		"full":          doContainer(ContainerBitmap, bitmapFull()),
		"firstBitSet":   doContainer(ContainerBitmap, bitmapFirstBitSet()),
		"lastBitSet":    doContainer(ContainerBitmap, bitmapLastBitSet()),
		"firstBitUnset": doContainer(ContainerBitmap, bitmapFirstBitUnset()),
		"lastBitUnset":  doContainer(ContainerBitmap, bitmapLastBitUnset()),
		"innerBitsSet":  doContainer(ContainerBitmap, bitmapInnerBitsSet()),
		"outerBitsSet":  doContainer(ContainerBitmap, bitmapOuterBitsSet()),
	}

	// run containers
	cts[ContainerRun] = map[string]*container{
		"empty":         doContainer(ContainerRun, runEmpty()),
		"full":          doContainer(ContainerRun, runFull()),
		"firstBitSet":   doContainer(ContainerRun, runFirstBitSet()),
		"lastBitSet":    doContainer(ContainerRun, runLastBitSet()),
		"firstBitUnset": doContainer(ContainerRun, runFirstBitUnset()),
		"lastBitUnset":  doContainer(ContainerRun, runLastBitUnset()),
		"innerBitsSet":  doContainer(ContainerRun, runInnerBitsSet()),
		"outerBitsSet":  doContainer(ContainerRun, runOuterBitsSet()),
	}

	return cts
}
