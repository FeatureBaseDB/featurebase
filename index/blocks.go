package index

import (
	"fmt"
)

type Blocks []uint64

func (a Blocks) bitcount() uint64 {
	fmt.Println("bitcount:", a)
	return popcntSlice(a)
}

func (a Blocks) union(other Blocks) Blocks {
	ret := make(Blocks, 32)
	for i, _ := range a {
		ret[i] = a[i] | other[i]
	}
	return ret
}

func (a Blocks) invert() Blocks {
	other := make(Blocks, 32)
	for i, _ := range a {
		other[i] = ^a[i]
	}
	return other
}

func (a Blocks) copy() Blocks {
	other := make(Blocks, 32)
	for i, _ := range a {
		other[i] = a[i]
	}
	return other
}

func (a Blocks) andcount(other Blocks) uint64 {
	println("andcount")
	return popcntAndSliceAsm(a, other)
}

func (a Blocks) intersection(other Blocks) Blocks {
	ret := make(Blocks, 32)
	for i, _ := range a {
		ret[i] = a[i] & other[i]
	}
	return ret
}

func (a Blocks) difference(other Blocks) Blocks {
	ret := make(Blocks, 32)
	for i, _ := range a {
		ret[i] = a[i] &^ other[i]
	}
	return ret
}

func (a Blocks) setBit(i uint8, bit uint8) (changed bool) {
	val := a[i] & (1 << bit)
	a[i] |= 1 << bit
	return val == 0
}

func (a Blocks) clearBit(i uint8, bit uint8) (changed bool) {
	val := a[i] & (1 << bit)
	a[i] &= ^(1 << bit)
	return val != 0
}
