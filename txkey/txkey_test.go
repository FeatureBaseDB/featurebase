// Copyright 2021 Molecula Corp. All rights reserved.
package txkey

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
)

func Test_KeyPrefix(t *testing.T) {

	// Prefix() must agree with Key(), but not have the key at the end.
	// This is important for iteration over containers.

	index, field, view, shard := "i", "f", "v", uint64(0)

	needle := Key(index, field, view, shard, 0)

	// prefix example: i%f;v:12345678<
	prefix := Prefix(index, field, view, shard)

	if !bytes.HasPrefix(needle, prefix) {
		panic(fmt.Sprintf("Prefix() output '%v'was not a prefix of Key() '%v'", string(needle), string(prefix)))
	}
	npre := len(prefix)
	nneed := len(needle)
	if npre+9 != nneed {
		panic(fmt.Sprintf("Prefix() output len %v '%v' was not 9 characters shorter than Key() len %v '%v'", npre, string(prefix), nneed, string(needle)))
	}

	// validate assumption that KeyExtractContainerKey() makes about strconv.ParseUint() error reporting;
	// for distinguishing prefixes from full keys. Even if the shard number is so large that the prefix
	// starts with a legitimate decimal number.
	shouldNotParse := "12345123451234';key<"
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
	key := []byte("~i%f;v:12345678<12345678#")
	binary.BigEndian.PutUint64(key[7:15], 1)

	if ShardFromKey(key) != 1 {
		panic("problem")
	}
	binary.BigEndian.PutUint64(key[7:15], 0)
	if ShardFromKey(key) != 0 {
		panic("problem")
	}
	binary.BigEndian.PutUint64(key[7:15], 18446744073709551615)
	if ShardFromKey(key) != 18446744073709551615 {
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
		ShardFromKey([]byte("~i%f;v:12345678<1234567#"))
	}()

}

func Test_PrefixFromKey(t *testing.T) {
	k := []byte("~i%f;v:12345678<12345678#")
	x := []byte("~i%f;v:12345678<")
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

func Test_Split(t *testing.T) {
	bkey := []byte("~i%f;v:12345678<12345678#")
	var xshard uint64 = 18446744073709551615
	var xckey uint64 = 43
	binary.BigEndian.PutUint64(bkey[7:15], xshard)
	binary.BigEndian.PutUint64(bkey[16:24], xckey)

	i, f, v, shard, ckey := Split(bkey)
	if i != "i" {
		panic("wrong index")
	}
	if f != "f" {
		panic("wrong field")
	}
	if v != "v" {
		panic("wrong view")
	}
	if shard != xshard {
		panic("wrong shard")
	}
	if ckey != xckey {
		panic("wrong ckey")
	}
}
