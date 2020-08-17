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

// Package txkey consolidates in one place the use of keys to index into our
// various storage/txn back-ends. Databases badgerDB and rbfDB both use it,
// so that debug Dumps are comparable.
package txkey

import (
	"bytes"
	"fmt"
	"strconv"
)

// Key produces the bytes that we use as a key to query the storage/tx engine.
// The roaringContainerKey argument is a container key into a roaring Container.
// Output examples:
//
// "idx:'i';fld:'f';vw:'standard';shd:'0';ckey@00000000000000000000" // smallest container-key
// "idx:'i';fld:'f';vw:'standard';shd:'0';ckey@18446744073709551615" // largest  container-key (math.MaxUint64)
//
// NB must be kept in sync with Prefix() and KeyExtractContainerKey().
//
func Key(index, field, view string, shard uint64, roaringContainerKey uint64) []byte {
	// The %020d which adds zero padding up to 20 runes is required to
	// allow the textual sort to accurately
	// reflect a numeric sort order. This is because, as a string,
	// math.MaxUint64 is 20 bytes long.
	// Example of such a Key with a container-key that is math.MaxUint64:
	// ...........................................12345678901234567890
	// idx:'i';fld:'f';vw:'standard';shd:'1';ckey@18446744073709551615

	prefix := Prefix(index, field, view, shard)
	ckey := []byte(fmt.Sprintf("%020d", roaringContainerKey))
	bkey := append(prefix, ckey...)
	MustValidateKey(bkey)
	return bkey
}

var ckeyPartExpected = []byte(";ckey@")

// MustValidatekey will panic on a bad Key with an informative message.
func MustValidateKey(bkey []byte) {
	n := len(bkey)
	if n < 56 {
		panic(fmt.Sprintf("bkey too short min size is 56 but we see %v in '%v'", n, string(bkey)))
	}
	beforeCkey := bkey[n-26 : n-20]
	if !bytes.Equal(beforeCkey, ckeyPartExpected) {
		panic(fmt.Sprintf(`bkey did not have expected ";ckey@" at 26 bytes from the end of the bkey '%v'; instead had '%v'`, string(bkey), string(beforeCkey)))
	}
}

func ShardFromKey(bkey []byte) (shard uint64) {
	MustValidateKey(bkey)

	n := len(bkey)
	// idx:'i';fld:'f';vw:'standard';shd:'1';ckey@18446744073709551615 -> idx:'i';fld:'f';vw:'standard';shd:'1
	by := bkey[:n-27]
	beg := bytes.LastIndex(by, []byte("'"))
	if beg == -1 {
		panic(fmt.Sprintf("bad bkey='%v' did not have single quote to being shard decoding", string(bkey)))
	}
	parseMe := string(by[beg+1:])
	shard, err := strconv.ParseUint(parseMe, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("could not parse parseMe '%v' in strconv.ParseUint(), error: '%v'", parseMe, err))
	}
	return shard
}

func ShardFromPrefix(prefix []byte) (shard uint64) {

	n := len(prefix)
	// idx:'i';fld:'f';vw:'standard';shd:'1';ckey@ -> idx:'i';fld:'f';vw:'standard';shd:'1
	by := prefix[:n-7]
	beg := bytes.LastIndex(by, []byte("'"))
	if beg == -1 {
		panic(fmt.Sprintf("bad prefix='%v' did not have single quote to being shard decoding", string(prefix)))
	}
	parseMe := string(by[beg+1:])
	shard, err := strconv.ParseUint(parseMe, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("could not parse parseMe '%v' in strconv.ParseUint(), error: '%v'", parseMe, err))
	}
	return shard
}

// KeyAndPrefix returns the equivalent of Key() and Prefix() calls.
func KeyAndPrefix(index, field, view string, shard uint64, roaringContainerKey uint64) (key, prefix []byte) {
	prefix = Prefix(index, field, view, shard)
	ckey := []byte(fmt.Sprintf("%020d", roaringContainerKey))
	bkey := append(prefix, ckey...)
	MustValidateKey(bkey)
	return bkey, prefix
}

var _ = KeyAndPrefix // keep linter happy

// KeyExtractContainerKey extracts the containerKey from bkey.
func KeyExtractContainerKey(bkey []byte) (containerKey uint64) {
	MustValidateKey(bkey)
	// The zero padding means that the container-key is always the last 20 bytes of the bkey.
	//
	// Be sure to catch the problematic case of a user passing in only a prefix. A prefix
	// ends in 'key@' rather than a full key that has 'key@00000000000000000001' (for example)
	// at the end. The ParseUint call below will fail in that case.
	n := len(bkey)
	if n < 20 {
		panic(fmt.Sprintf("KeyExtractContainerKey() error: bad bkey '%v', too short!", string(bkey)))
	}
	last := bkey[n-20:] // Key() and Prefix() always return more than 20 rune []byte.
	var err error
	containerKey, err = strconv.ParseUint(string(last), 10, 64) // has to be the container key
	if err != nil {
		panic(fmt.Sprintf("KeyExtractContainerKey() error: bad bkey '%v', could not convert last 20 bytes ('%v') to a unit64: '%v'", string(bkey), string(last), err))
	}
	return
}

func AllShardPrefix(index, field, view string) []byte {
	return []byte(fmt.Sprintf("idx:'%v';fld:'%v';vw:'%v';shd:", index, field, view))
}

// Prefix returns everything from Key up to and
// including the '@' fune in a Key. The prefix excludes the roaring container key itself.
// NB must be kept in sync with Key() and KeyExtractContainerKey().
func Prefix(index, field, view string, shard uint64) []byte {
	return []byte(fmt.Sprintf("idx:'%v';fld:'%v';vw:'%v';shd:'%020v';ckey@", index, field, view, shard))
}

// IndexOnlyPrefix returns a prefix suitable for DeleteIndex and a key-scan to
// remove all storage associated with one index.
//
// The full name of the index must be provided, no partial index names will work.
//
// The provided key is terminated by `';` and so DeleteIndex("i") will not delete the index "i2".
//
func IndexOnlyPrefix(indexName string) []byte {
	return []byte(fmt.Sprintf("idx:'%v';", indexName))
}

// same for deleting a whole field.
func FieldPrefix(index, field string) []byte {
	return []byte(fmt.Sprintf("idx:'%v';fld:'%v';", index, field))
}

func PrefixFromKey(bkey []byte) (prefix []byte) {
	MustValidateKey(bkey)
	beg := bytes.LastIndex(bkey, []byte("@"))
	if beg == -1 {
		panic(fmt.Sprintf("bad bkey='%v' did not have '@' extract prefix", string(bkey)))
	}
	return bkey[:beg+1]
}
