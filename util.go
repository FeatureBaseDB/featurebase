// Copyright 2020 Pilosa Corp.
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

package pilosa

// util.go: a place for generic, reusable utilities.

import (
	"fmt"
	"io/ioutil"
	"net"
	"reflect"
	"sort"
	"unsafe"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

// LeftShifted16MaxContainerKey is 0xffffffffffff0000. It is similar
// to the roaring.maxContainerKey  0x0000ffffffffffff, but
// shifted 16 bits to the left so its domain is the full [0, 2^64) bit space.
// It is used to match the semantics of the roaring.OffsetRange() API.
// This is the maximum endx value for Tx.OffsetRange(), because the lowbits,
// as in the roaring.OffsetRange(), are not allowed to be set.
// It is used in Tx.RoaringBitamp() to obtain the full contents of a fragment
// from a call from tx.OffsetRange() by requesting [0, LeftShifted16MaxContainerKey)
// with an offset of 0.
const LeftShifted16MaxContainerKey = uint64(0xffffffffffff0000) // or math.MaxUint64 - (1<<16 - 1), or 18446744073709486080

// NilInside checks if the provided iface is nil or
// contains a nil pointer, slice, array, map, or channel.
func NilInside(iface interface{}) bool {
	if iface == nil {
		return true
	}
	switch reflect.TypeOf(iface).Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Array, reflect.Map, reflect.Chan:
		return reflect.ValueOf(iface).IsNil()
	}
	return false
}

// GetAvailPort asks the OS for an unused port.
// There's a race here, where the port could be grabbed by someone else
// before the caller gets to Listen on it, but we are only using
// it to find a random port for the test hang debugging.
// Moreover, in practice such races are rare. Just ask for
// it again if the port is taken.
// Uses net.Listen("tcp", ":0") to determine a free port, then
// releases it back to the OS with Listener.Close().
func GetAvailPort() int {
	l, _ := net.Listen("tcp", ":0")
	r := l.Addr()
	l.Close()
	return r.(*net.TCPAddr).Port
}

//////////////////////////////////
// helper utility functions

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

func toArray16(a []byte) []uint16 {
	return (*[4096]uint16)(unsafe.Pointer(&a[0]))[: len(a)/2 : len(a)/2]
}
func toArray64(a []byte) []uint64 {
	return (*[1024]uint64)(unsafe.Pointer(&a[0]))[:1024:1024]
}
func toInterval16(a []byte) []roaring.Interval16 {
	return (*[2048]roaring.Interval16)(unsafe.Pointer(&a[0]))[: len(a)/4 : len(a)/4]
}

func sliceToMap(slc []uint64) (m map[uint64]bool) {
	m = make(map[uint64]bool)
	for _, v := range slc {
		m[v] = true
	}
	return
}

// return A - B
func mapDiff(mapA, mapB map[uint64]bool) (r []int) {
	for a := range mapA {
		_, ok := mapB[a]
		if !ok {
			r = append(r, int(a))
		}
	}
	return
}

func asInts(a []uint64) (r []int) {
	r = make([]int, len(a))
	for i, v := range a {
		r[i] = int(v)
	}
	return
}

func containerAsString(ckey uint64, rc *roaring.Container) (r string) {
	rbm := roaring.NewBitmap()
	rbm.Containers.Put(ckey, rc)
	return BitmapAsString(rbm)
}

var _ = containerAsString // happy linter

func roaringBitmapDiff(a, b *roaring.Bitmap) error {
	nA := a.Count()
	nB := b.Count()

	slcA := a.Slice()
	slcB := b.Slice()

	mapA := sliceToMap(slcA)
	mapB := sliceToMap(slcB)

	AminusB := mapDiff(mapA, mapB)
	BminusA := mapDiff(mapB, mapA)

	sort.Ints(AminusB)
	sort.Ints(BminusA)

	res := fmt.Sprintf("nA = %v; nB = %v;\n", nA, nB)
	ndiff := 0
	if nA != nB {
		ndiff++
	}

	if len(AminusB) > 0 {
		res += fmt.Sprintf("==> AminusB = (len %v) '%#v'; ", len(AminusB), AminusB)
		ndiff++
	}
	if len(BminusA) > 0 {
		res += fmt.Sprintf("\n==> BminusA = (len %v) '%#v'; ", len(BminusA), BminusA)
		ndiff++
	}
	if ndiff == 0 {
		return nil
	}
	res += fmt.Sprintf("\n ==> A = '%#v'\n ==> B = '%#v'", asInts(slcA), asInts(slcB))
	return errors.New(res)
}

func dirAsString(path string) (r string) {
	r = fmt.Sprintf("dump of directory '%v':\n", path)
	files, err := ioutil.ReadDir(path)
	panicOn(err)
	for _, f := range files {
		r += f.Name() + "\n"
	}
	return r
}

var _ = dirAsString // happy linter

var _ = zeroKeyContainerAsString // happy linter

// for debugging
func zeroKeyContainerAsString(ct *roaring.Container) (r string) {
	cts := roaring.NewSliceContainers()
	cts.Put(0, ct)
	rbm := &roaring.Bitmap{Containers: cts}
	r = fmt.Sprintf("[%v]:", containerTypeNames[roaring.ContainerType(ct)]) + BitmapAsString(rbm)
	return
}

var containerTypeNames = map[byte]string{
	roaring.ContainerArray:  "array",
	roaring.ContainerBitmap: "bitmap",
	roaring.ContainerRun:    "run",
}

func BitmapAsString(rbm *roaring.Bitmap) (r string) {
	r = "c("
	slc := rbm.Slice()
	width := 0
	s := ""
	for _, v := range slc {
		if width == 0 {
			s = fmt.Sprintf("%v", v)
		} else {
			s = fmt.Sprintf(", %v", v)
		}
		width += len(s)
		r += s
		if width > 70 {
			r += ",\n"
			width = 0
		}
	}
	if width == 0 && len(r) > 2 {
		r = r[:len(r)-2]
	}
	return r + ")"
}

// fromArray16 converts to an 8KB page
func fromArray16(a []uint16) []byte {
	if len(a) == 0 {
		return []byte{}
	}
	if len(a) > 4096 {
		panic(fmt.Sprintf("cannot put more than 4096 integers into an array container: %v too big", len(a)))
	}
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[: len(a)*2 : len(a)*2]
}

// fromArray64 converts to an 8KB page
func fromArray64(a []uint64) []byte {
	if len(a) == 0 {
		return []byte{}
	}
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[:8192:8192]
}

// fromInterval16 converts to 8KB page
func fromInterval16(a []roaring.Interval16) []byte {
	if len(a) == 0 {
		return []byte{}
	}
	if len(a) > 2048 {
		panic(fmt.Sprintf("cannot put more than 2048 roaring.Interval16 into a container: %v too big", len(a)))
	}
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[: len(a)*4 : len(a)*4]
}
