// Copyright 2019 Pilosa Corp.
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

import (
	"fmt"

	"github.com/pilosa/pilosa/v2/ext"
	"github.com/pilosa/pilosa/v2/roaring"
)

// WrapBitmap yields an extension-Bitmap from a roaring Bitmap.
func WrapBitmap(bm *roaring.Bitmap) ext.Bitmap {
	return wrappedBitmap{bm}
}

// wrappedBitmap is a very shallow glue shim to convert a roaring Bitmap to
// an extension Bitmap.
type wrappedBitmap struct{ *roaring.Bitmap }

// UnwrapBitmap converts an extension-bitmap to its underlying roaring Bitmap.
func UnwrapBitmap(bm ext.Bitmap) *roaring.Bitmap {
	if inner, ok := bm.(wrappedBitmap); ok {
		if inner.Bitmap != nil {
			return inner.Bitmap
		}
		return roaring.NewFileBitmap()
	}
	return roaring.NewFileBitmap()
}

func (b wrappedBitmap) Intersect(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Intersect(other.(wrappedBitmap).Bitmap)}
}

func (b wrappedBitmap) Union(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Union(other.(wrappedBitmap).Bitmap)}
}

func (b wrappedBitmap) IntersectionCount(other ext.Bitmap) uint64 {
	return b.Bitmap.IntersectionCount(other.(wrappedBitmap).Bitmap)
}

func (b wrappedBitmap) Difference(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Difference(other.(wrappedBitmap).Bitmap)}
}

func (b wrappedBitmap) Xor(other ext.Bitmap) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Xor(other.(wrappedBitmap).Bitmap)}
}

func (b wrappedBitmap) Shift(n int) (ext.Bitmap, error) {
	shifted, err := b.Bitmap.Shift(n)
	return wrappedBitmap{shifted}, err
}

func (b wrappedBitmap) Flip(start, last uint64) ext.Bitmap {
	return wrappedBitmap{b.Bitmap.Flip(start, last)}
}

func (b wrappedBitmap) New() ext.Bitmap {
	return WrapBitmap(roaring.NewFileBitmap())
}

// ContainerBits tries to get one container's worth of bits.
func (b wrappedBitmap) ContainerBits(offset uint64, target []uint64) (out []uint64) {
	// it's an error to call this with a non-container-aligned offset
	if offset&0xFFFF != 0 {
		return nil
	}
	if b.Bitmap == nil {
		fmt.Printf("ContainerBits on bitmap with no contents\n")
		return nil
	}
	if b.Bitmap.Containers == nil {
		fmt.Printf("ContainerBits on bitmap with nil Containers\n")
		return nil
	}
	c := b.Bitmap.Containers.Get(offset >> 16)
	if c == nil {
		return nil
	}
	return c.AsBitmap(target)
}
