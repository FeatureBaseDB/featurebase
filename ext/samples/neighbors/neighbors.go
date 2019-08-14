package main

import (
	"fmt"

	"github.com/pilosa/pilosa/ext"
)

// BitmapOps describes the provided ops to the server.
func BitmapOps() ([]ext.BitmapOp, error) {
	return []ext.BitmapOp{
		{Name: "CountNeighbors", Arity: ext.OpUnary, CountFunc: CountNeighbors},
		{Name: "Neighbors", Arity: ext.OpUnary, BitmapFunc: Neighbors},
	}, nil
}

// CountNeighbors only checks its first parameter, because it's described as unary.
func CountNeighbors(inputs []ext.Bitmap) uint64 {
	in := inputs[0]
	shifted, _ := in.Shift(1)
	prime := in.Intersect(shifted)
	unshifted, _ := prime.Shift(-1)
	return unshifted.Union(prime).Count()
}

// Neighbors only checks its first parameter, because it's described as unary.
func Neighbors(inputs []ext.Bitmap) ext.Bitmap {
	in := inputs[0]
	shifted, _ := in.Shift(1)
	prime := in.Intersect(shifted)
	unshifted, _ := prime.Shift(-1)
	return unshifted.Union(prime)
}

func main() {
	fmt.Printf("this is a plugin module only.\n")
}
