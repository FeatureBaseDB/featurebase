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

package main

import (
	"fmt"
	"math/bits"

	"github.com/molecula/apophenia"
	"github.com/pilosa/pilosa/ext"
)

// This could be dynamically generated, but for now it's not.
//lint:ignore U1000 Plugins are called externally later.
var extInfoTemplate = &ext.ExtensionInfo{
	Name:         "some",
	Description:  "some of the bits/all of the bits/none of the bits",
	Version:      "0.01",
	ExtensionAPI: "v0",
	License:      "unreleased",
	BitmapOps: []ext.BitmapOp{
		{Name: "Some", Func: ext.BitmapOpUnaryBitmap(Some), Reserved: []string{"p", "seed"}},
	},
}

// ExtensionInfo is the entry point used by the plugin code.
//lint:ignore U1000 Plugins are called externally later.
func ExtensionInfo(api string) (*ext.ExtensionInfo, error) { // nolint: deadcode
	return extInfoTemplate, nil
}

//lint:ignore U1000 Plugins are called externally later.
const batchSize = 1024

// Some returns some of the bits from its first input bitmap. Takes seed (int)
// and p (float) values. Seed defaults to 0.
//lint:ignore U1000 Plugins are called externally later.
func Some(inputs []ext.Bitmap, args map[string]interface{}) ext.Bitmap { // nolint: deadcode
	if len(inputs) == 0 || inputs[0] == nil {
		return nil
	}
	input := inputs[0]
	min, ok := input.Min()
	// no bits found?
	if !ok {
		return nil
	}
	// start at multiple of 128 not greater than min.
	min &^= 127
	max := input.Max()
	pArg, ok := args["p"]
	if !ok {
		return nil
	}
	p, ok := pArg.(float64)
	if !ok {
		return nil
	}
	// no bits or impossible probability range
	if p <= 0 || p > 1 {
		return nil
	}
	// every bit
	if p == 1 {
		return inputs[0]
	}
	var seed int64
	seedArg, ok := args["seed"]
	if ok {
		// it's fine if this fails; we'll default to 0.
		seed, _ = seedArg.(int64)
	}
	densityScale := uint64(256)
	density := uint64(p * float64(densityScale))
	for density == 0 {
		densityScale <<= 1
		density = uint64(p * float64(densityScale))
		// too small
		if densityScale > (1 << 32) {
			return nil
		}
	}
	w, err := apophenia.NewWeighted(apophenia.NewSequence(seed))
	if err != nil {
		return nil
	}
	someBits := input.New()
	toAdd := make([]uint64, batchSize)
	toAddN := 0
	offset := apophenia.OffsetFor(apophenia.SequenceWeighted, 0, 0, 0)
	for i := min; i < max; i += 128 {
		offset.Lo = i
		randomBits := w.Bits(offset, density, densityScale)
		bit := uint64(0)
		for randomBits.Lo != 0 {
			next := uint64(bits.TrailingZeros64(randomBits.Lo) + 1)
			randomBits.Lo >>= next
			toAdd[toAddN] = next + bit + i
			toAddN++
			bit += next
		}
		bit = 64
		for randomBits.Hi != 0 {
			next := uint64(bits.TrailingZeros64(randomBits.Hi) + 1)
			randomBits.Hi >>= next
			toAdd[toAddN] = next + bit + i
			toAddN++
			bit += next
		}
		if toAddN > (batchSize - 128) {
			// ignore error
			_, _ = someBits.AddN(toAdd[:toAddN]...)
			toAddN = 0
		}
	}
	if toAddN > 0 {
		_, _ = someBits.AddN(toAdd[:toAddN]...)
	}
	return input.Intersect(someBits)
}

func main() {
	fmt.Printf("this is a plugin module only.\n")
}
