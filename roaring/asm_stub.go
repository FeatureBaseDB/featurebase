package roaring

//go:noescape
func asmAnd(a,b,c []uint64)int
func asmOr(a,b,c []uint64)int
func asmXor(a,b,c []uint64)int
func asmAndN(a,b,c []uint64)int



