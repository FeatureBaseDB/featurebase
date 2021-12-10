// Copyright 2021 Molecula Corp. All rights reserved.
package short_txkey

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func Test_KeyPrefix(t *testing.T) {

	// Prefix() must agree with Key(), but not have the key at the end.
	// This is important for iteration over containers.

	index, field, view := "i", "f", "v"

	needle := Key(index, field, view, 0, 0)

	// prefix example: i%f;v:12345678<
	prefix := Prefix(index, field, view, 0)

	//fmt.Printf("needle = '%v'\n", string(needle))
	//fmt.Printf("prefix = '%v'\n", string(prefix))

	if !bytes.HasPrefix(needle, prefix) {
		panic(fmt.Sprintf("Prefix() output '%v'was not a prefix of Key() '%v'", string(needle), string(prefix)))
	}
	npre := len(prefix)
	nneed := len(needle)
	if npre+9 != nneed {
		panic(fmt.Sprintf("Prefix() output len %v '%v' was not 9 characters shorter than Key() len %v '%v'", npre, string(prefix), nneed, string(needle)))
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

func Test_PrefixFromKey(t *testing.T) {
	k := []byte("~f;v<12345678#")
	x := []byte("~f;v<")
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
	bkey := []byte("~f;v<12345678#")
	var xckey uint64 = 43
	binary.BigEndian.PutUint64(bkey[5:13], xckey)

	f, v, ckey := Split(bkey)
	if f != "f" {
		panic("wrong field")
	}
	if v != "v" {
		panic("wrong view")
	}
	if ckey != xckey {
		panic("wrong ckey")
	}
}
