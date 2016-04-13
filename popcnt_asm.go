// +build amd64

package pilosa

//go:noescape

func hasAsm() bool

var useAsm = hasAsm()

//go:noescape

func popcntSliceAsm(s []uint64) uint64

//go:noescape

func popcntMaskSliceAsm(s, m []uint64) uint64

//go:noescape

func popcntAndSliceAsm(s, m []uint64) uint64

//go:noescape

func popcntOrSliceAsm(s, m []uint64) uint64

//go:noescape

func popcntXorSliceAsm(s, m []uint64) uint64

//go:noescape
func popcntAsm(x uint64) uint64

func popcntSlice(s []uint64) uint64 {
	if useAsm {
		return popcntSliceAsm(s)
	}
	return popcntSliceGo(s)
}

func popcntMaskSlice(s, m []uint64) uint64 {
	if useAsm {
		return popcntMaskSliceAsm(s, m)
	}
	return popcntMaskSliceGo(s, m)
}

func popcntAndSlice(s, m []uint64) uint64 {
	if useAsm {
		return popcntAndSliceAsm(s, m)
	}
	return popcntAndSliceGo(s, m)
}

func popcntOrSlice(s, m []uint64) uint64 {
	if useAsm {
		return popcntOrSliceAsm(s, m)
	}
	return popcntOrSliceGo(s, m)
}

func popcntXorSlice(s, m []uint64) uint64 {
	if useAsm {
		return popcntXorSliceAsm(s, m)
	}
	return popcntXorSliceGo(s, m)
}

func popcnt(x uint64) uint64 {
	if useAsm {
		return popcntAsm(x)
	}
	return popcntGo(x)
}
