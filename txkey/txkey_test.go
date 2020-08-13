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

package txkey

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
)

func Test_KeyPrefix(t *testing.T) {

	// Prefix() must agree with Key(), but not have the key at the end.
	// This is important for iteration over containers.

	index, field, view, shard := "i", "f", "v", uint64(0)

	// needle examples with the container-key extremes:
	// "index:'i';field:'f';view:'v';shard:'0';key@00000000000000000000" // smallest
	// "index:'i';field:'f';view:'v';shard:'0';key@18446744073709551615" // largest
	needle := Key(index, field, view, shard, 0)

	// prefix example: "index:'i';field:'f';view:'v';shard:'0';key@"
	prefix := Prefix(index, field, view, shard)

	if !bytes.HasPrefix(needle, prefix) {
		panic(fmt.Sprintf("Prefix() output '%v'was not a prefix of Key() '%v'", string(needle), string(prefix)))
	}
	if len(prefix)+20 != len(needle) {
		panic(fmt.Sprintf("Prefix() output '%v'was 20 characters shorter than Key() '%v'", string(needle), string(prefix)))
	}

	// validate assumption that KeyExtractContainerKey() makes about strconv.ParseUint() error reporting;
	// for distinguishing prefixes from full keys. Even if the shard number is so large that the prefix
	// starts with a legitimate decimal number.
	shouldNotParse := "12345123451234';key@"
	containerKey, err := strconv.ParseUint(shouldNotParse, 10, 64)
	if err == nil {
		panic(fmt.Sprintf("strconv.ParseUint should have returned an error parsing this string '%v'; instead we got '%v'", shouldNotParse, containerKey))
	}

	// verify panic on submitting a prefix
	func() {
		defer func() {
			r := recover()
			if r == nil {
				panic(fmt.Sprintf("should have seen panic on call to KeyExtractContainerKey(prefix='%v')", prefix))
			}
		}()
		KeyExtractContainerKey(prefix) // should panic.
	}()
}

func Test_ShardFromKey(t *testing.T) {
	if ShardFromKey([]byte("idx:'i';fld:'f';vw:'standard';shd:'1';ckey@18446744073709551615")) != 1 {
		panic("problem")
	}
	if ShardFromKey([]byte("idx:'i';fld:'f';vw:'standard';shd:'0';ckey@18446744073709551615")) != 0 {
		panic("problem")
	}
	if ShardFromKey([]byte("idx:'i';fld:'f';vw:'standard';shd:'18446744073709551615';ckey@18446744073709551615")) != 18446744073709551615 {
		panic("problem")
	}

	func() {
		defer func() {
			r := recover()
			if r == nil {
				panic("should have panic-ed")
			}
		}()
		// called for the panic of a short ckey, only 19 bytes instead of 20
		ShardFromKey([]byte("idx:'i';fld:'f';vw:'standard';shd:'18446744073709551615';ckey@1844674407370955161"))
	}()

}

func Test_PrefixFromKey(t *testing.T) {
	k := []byte("idx:'i';fld:'f';vw:'standard';shd:'1';ckey@18446744073709551615")
	x := []byte("idx:'i';fld:'f';vw:'standard';shd:'1';ckey@")
	pre := PrefixFromKey(k)
	if !bytes.Equal(pre, x) {
		nx := len(x)
		npre := len(pre)
		if nx != npre {
			panic(fmt.Sprintf("nx=%v, npre=%v; expected '%v', observed '%v'", nx, npre, string(x), string(pre)))
		}
		for i := 0; i < nx; i++ {
			if x[i] != pre[i] {
				panic(fmt.Sprintf("first diff at index %v, expected '%v', observed '%v'", i, string(x[:i]), string(pre[:i])))
			}
		}
		panic(fmt.Sprintf("expected:\n%v\n, observed:\n%v\n", string(x), string(pre)))
	}
}
