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

import (
	"fmt"
	"testing"

	"encoding/hex"
)

func TestBlake3Hasher(t *testing.T) {

	hasher := NewBlake3Hasher()
	hash := make([]byte, 16)
	input := []byte("hello world")
	hash = hasher.CryptoHash(input, hash)
	expected := "d74981efa70a0c880b8d8c1985d075db"
	observed := hex.EncodeToString(hash)
	if observed != expected {
		panic(fmt.Sprintf("expected hash:'%v' but observed hash '%v'", expected, observed))
	}

	obs2 := blake3sum16(input)
	if obs2 != expected {
		panic(fmt.Sprintf("expected hash:'%v' but observed hash from blake2sum16: '%v'", expected, obs2))
	}
}

func TestCryptoRandInt64(t *testing.T) {
	rnd := cryptoRandInt64()
	if rnd == 0 {
		panic("cryptoRandInt64() gave 0, very high odds it has broken")
	}
}
