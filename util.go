// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

// util.go: a place for generic, reusable utilities.

import (
	"os"
	"reflect"
	"syscall"
	"time"

	"github.com/molecula/featurebase/v3/roaring"
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

//////////////////////////////////
// helper utility functions

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

// called by Holder.hasRoaringData()
func roaringFragmentHasData(path string, index, field, view string, shard uint64) (hasData bool, err error) {

	var info roaring.BitmapInfo
	_ = info
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return
	}

	var fi os.FileInfo
	fi, err = f.Stat()
	if err != nil {
		return
	}

	// Memory map the file.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		err = errors.Wrap(err, "mmapping")
		return
	}
	defer func() {
		err = syscall.Munmap(data)
		if err != nil {
			err = errors.Wrap(err, "roaringFragmentHasData: munmap failed")
		}
		err = f.Close()
		if err != nil {
			err = errors.Wrap(err, "roaringFragmentHasData f.Close() in defer")
		}
	}()

	// Attach the mmap file to the bitmap.
	var rbm *roaring.Bitmap
	rbm, _, err = roaring.InspectBinary(data, true, &info)
	if err != nil {
		err = errors.Wrap(err, "inspecting")
		return
	}

	if info.ContainerCount > 0 {
		return true, nil
	}
	if info.Ops > 0 {
		return true, nil
	}

	citer, found := rbm.Containers.Iterator(0)
	_ = found

	for citer.Next() {
		return true, nil
	}

	return
}

// GetLoopProgress returns the estimated remaining time to iterate through some
// items as well as the loop completion percentage with the following
// parameters:
// the start time, the current time, the iteration, and the number of items
func GetLoopProgress(start time.Time, now time.Time, iteration uint, total uint) (remaining time.Duration, pctDone float64) {
	itemsLeft := total - (iteration + 1)
	avgItemTime := float64(now.Sub(start)) / float64(iteration+1)
	pctDone = (float64(iteration+1) / float64(total)) * 100
	return time.Duration(avgItemTime * float64(itemsLeft)), pctDone
}

// FormatTimestampNano returns the string representation of a timestamp given:
// an epoch value, base, and time unit
func FormatTimestampNano(value, base int64, timeUnit string) string {
	return time.Unix(0, (value+base)*TimeUnitNanos(timeUnit)).UTC().Format(time.RFC3339Nano)
}
