// Copyright 2021 Molecula Corp. All rights reserved.
package roaring

import "sync"

///////////////////////////////////////////////////////////////////////////

const containerWidth = 1 << 16

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
	return []uint16{0, 65535}
}

func arrayOddBitsSet() []uint16 {
	array := make([]uint16, containerWidth/2)
	for i := 0; i < int(containerWidth/2); i++ {
		array[i] = uint16(2*i + 1)
	}
	return array
}

func arrayEvenBitsSet() []uint16 {
	array := make([]uint16, containerWidth/2)
	for i := 0; i < int(containerWidth/2); i++ {
		array[i] = uint16(2 * i)
	}
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

func bitmapSecondBitSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[0] = 0x0000000000000002
	return bitmap
}

func bitmapLastBitFirstRowSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[0] = 0x8000000000000000
	return bitmap
}

func bitmapFirstBitSecoundRowSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[1] = 0x0000000000000001
	return bitmap
}

func bitmapLastBitSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	bitmap[bitmapN-1] = 0x8000000000000000
	return bitmap
}

func bitmapFirstBitUnset() []uint64 {
	bitmap := bitmapFull()
	bitmap[0] = 0xFFFFFFFFFFFFFFFE
	return bitmap
}

func bitmapLastBitUnset() []uint64 {
	bitmap := bitmapFull()
	bitmap[bitmapN-1] = 0x7FFFFFFFFFFFFFFF
	return bitmap
}

func bitmapInnerBitsSet() []uint64 {
	bitmap := bitmapFull()
	bitmap[0] = 0xFFFFFFFFFFFFFFFE
	bitmap[bitmapN-1] = 0x7FFFFFFFFFFFFFFF
	return bitmap
}

func bitmapOuterBitsSet() []uint64 {
	bitmap := bitmapEmpty()
	bitmap[0] = 0x0000000000000001
	bitmap[bitmapN-1] = 0x8000000000000000
	return bitmap
}

func bitmapOddBitsSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	for i := 0; i < bitmapN; i++ {
		bitmap[i] = 0xAAAAAAAAAAAAAAAA
	}
	return bitmap
}

func bitmapEvenBitsSet() []uint64 {
	bitmap := make([]uint64, bitmapN)
	for i := 0; i < bitmapN; i++ {
		bitmap[i] = 0x5555555555555555
	}
	return bitmap
}

////////////////// run
func runEmpty() []Interval16 {
	return make([]Interval16, 0)
}

func runFull() []Interval16 {
	run := make([]Interval16, 0)
	run = append(run, Interval16{Start: 0, Last: 65535})
	return run
}

func runFirstBitSet() []Interval16 {
	run := make([]Interval16, 0)
	run = append(run, Interval16{Start: 0, Last: 0})
	return run
}

func runLastBitSet() []Interval16 {
	run := make([]Interval16, 0)
	run = append(run, Interval16{Start: 65535, Last: 65535})
	return run
}

func runFirstBitUnset() []Interval16 {
	run := make([]Interval16, 0)
	run = append(run, Interval16{Start: 1, Last: 65535})
	return run
}

func runLastBitUnset() []Interval16 {
	run := make([]Interval16, 0)
	run = append(run, Interval16{Start: 0, Last: 65534})
	return run
}

func runInnerBitsSet() []Interval16 {
	run := make([]Interval16, 0)
	run = append(run, Interval16{Start: 1, Last: 65534})
	return run
}

func runOuterBitsSet() []Interval16 {
	run := make([]Interval16, 0)
	run = append(run, Interval16{Start: 0, Last: 0})
	run = append(run, Interval16{Start: 65535, Last: 65535})
	return run
}

func runOddBitsSet() []Interval16 {
	run := make([]Interval16, containerWidth/2)
	for i := 0; i < int(containerWidth/2); i++ {
		run[i] = Interval16{Start: uint16(2*i + 1), Last: uint16(2*i + 1)}
	}
	return run
}

func runEvenBitsSet() []Interval16 {
	run := make([]Interval16, containerWidth/2)
	for i := 0; i < int(containerWidth/2); i++ {
		run[i] = Interval16{Start: uint16(2 * i), Last: uint16(2 * i)}
	}
	return run
}

///////////////////////////////////////////////////////////////////////////

// f is a container function taking either one or two containers as input
// func(a *container) *container
// func(a, b *container) *container
type testOp struct {
	f   interface{}
	x   string
	y   string
	exp string
}

func doContainer(typ byte, data interface{}) *Container {
	switch typ {
	case ContainerArray:
		return NewContainerArray(data.([]uint16))
	case ContainerBitmap:
		c := NewContainerBitmap(-1, data.([]uint64))
		return c
	case ContainerRun:
		return NewContainerRun(data.([]Interval16))
	}
	return nil
}

var makeCts sync.Once
var sampleTestContainers map[byte]map[string]*Container

func setupContainerTests() map[byte]map[string]*Container {

	makeCts.Do(func() {
		sampleTestContainers = make(map[byte]map[string]*Container)

		// array containers
		sampleTestContainers[ContainerArray] = map[string]*Container{
			"empty":         doContainer(ContainerArray, arrayEmpty()),
			"full":          doContainer(ContainerArray, arrayFull()),
			"firstBitSet":   doContainer(ContainerArray, arrayFirstBitSet()),
			"lastBitSet":    doContainer(ContainerArray, arrayLastBitSet()),
			"firstBitUnset": doContainer(ContainerArray, arrayFirstBitUnset()),
			"lastBitUnset":  doContainer(ContainerArray, arrayLastBitUnset()),
			"innerBitsSet":  doContainer(ContainerArray, arrayInnerBitsSet()),
			"outerBitsSet":  doContainer(ContainerArray, arrayOuterBitsSet()),
			"oddBitsSet":    doContainer(ContainerArray, arrayOddBitsSet()),
			"evenBitsSet":   doContainer(ContainerArray, arrayEvenBitsSet()),
		}

		// bitmap containers
		sampleTestContainers[ContainerBitmap] = map[string]*Container{
			"empty":         doContainer(ContainerBitmap, bitmapEmpty()),
			"full":          doContainer(ContainerBitmap, bitmapFull()),
			"firstBitSet":   doContainer(ContainerBitmap, bitmapFirstBitSet()),
			"lastBitSet":    doContainer(ContainerBitmap, bitmapLastBitSet()),
			"firstBitUnset": doContainer(ContainerBitmap, bitmapFirstBitUnset()),
			"lastBitUnset":  doContainer(ContainerBitmap, bitmapLastBitUnset()),
			"innerBitsSet":  doContainer(ContainerBitmap, bitmapInnerBitsSet()),
			"outerBitsSet":  doContainer(ContainerBitmap, bitmapOuterBitsSet()),
			"oddBitsSet":    doContainer(ContainerBitmap, bitmapOddBitsSet()),
			"evenBitsSet":   doContainer(ContainerBitmap, bitmapEvenBitsSet()),
		}

		// run containers
		sampleTestContainers[ContainerRun] = map[string]*Container{
			"empty":         doContainer(ContainerRun, runEmpty()),
			"full":          doContainer(ContainerRun, runFull()),
			"firstBitSet":   doContainer(ContainerRun, runFirstBitSet()),
			"lastBitSet":    doContainer(ContainerRun, runLastBitSet()),
			"firstBitUnset": doContainer(ContainerRun, runFirstBitUnset()),
			"lastBitUnset":  doContainer(ContainerRun, runLastBitUnset()),
			"innerBitsSet":  doContainer(ContainerRun, runInnerBitsSet()),
			"outerBitsSet":  doContainer(ContainerRun, runOuterBitsSet()),
			"oddBitsSet":    doContainer(ContainerRun, runOddBitsSet()),
			"evenBitsSet":   doContainer(ContainerRun, runEvenBitsSet()),
		}
	})

	return sampleTestContainers
}
