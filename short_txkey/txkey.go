// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// Package txkey consolidates in one place the use of keys to index into our
// various storage/txn back-ends. The short_txkey version omits the
// index and shard, since these are implicitly part of our database-per-shard
// in an index scheme. In other words, every database is only in exactly
// one shard of one index already. There is no need to repeat the index
// and shard in these keys.
package short_txkey

import (
	"encoding/binary"
	"fmt"
)

// FieldView is here to avoid circular import.
type FieldView struct {
	Field string
	View  string
}

func FieldViewFromPrefix(prefix []byte) FieldView {
	field, view := SplitPrefix(prefix)
	return FieldView{Field: field, View: view}
}

func FieldViewFromFullKey(fullKey []byte) FieldView {
	field, view, _ := Split(fullKey)
	return FieldView{Field: field, View: view}
}

// Key produces the bytes that we use as a key to query the storage/tx engine.
// The roaringContainerKey argument to Key() is a container key into a roaring Container.
// The return value from Key() is constructed as follows:
//
//     ~field;view<ckey#
//
// where ckey is always exactly 8 bytes, uint64 big-endian encoded.
//
// Keys always start with either '~' or '>'. Keys always end with '#'.
// Keys always contain exactly one each of ';' and '<', in that order.
// The field is between the '~' and the ';'. It must be at least 1 byte long.
// The view is between the ';' and the '<'. It must be at least 1 byte long.
// The ckey is the 8 bytes between the '<' and the '#'.
// The Prefix of a key ends at, and includes, the '<'. It is at least 13 bytes long.
// The index, field, and view are not allowed to contain these reserved bytes:
//  {'~', '>', ';', ':', '<', '#', '$', '%', '^', '(', ')', '*', '!'}
//
// The bytes {'+', '/', '-', '_', '.', and '=' can be used in index, field, and view; to enable
// base-64 encoding.
//
// The shortest possible key is 14 bytes. It would be laid out like this:
//           ~f;v<12345678#
//           12345678901234
//
// keys starting with '~' are regular value keys.
// keys starting with '>' are symlink keys.
//
// NB must be kept in sync with Prefix() and KeyExtractContainerKey().
//
func Key(index, field, view string, shard, roaringContainerKey uint64) (r []byte) {

	prefix := Prefix(index, field, view, shard)

	var ckey [9]byte
	binary.BigEndian.PutUint64(ckey[:8], roaringContainerKey)
	ckey[8] = byte('#')
	return append(prefix, ckey[:]...)
}

// KeyAndPrefix returns the equivalent of Key() and Prefix() calls.
func KeyAndPrefix(index, field, view string, shard, roaringContainerKey uint64) (key, prefix []byte) {
	prefix = Prefix(index, field, view, shard)

	var ckey [9]byte
	binary.BigEndian.PutUint64(ckey[:8], roaringContainerKey)
	ckey[8] = byte('#')
	key = append(prefix, ckey[:]...)
	return
}

var _ = KeyAndPrefix // keep linter happy

func MustValidateKey(bkey []byte) {
	n := len(bkey)
	if n < 14 {
		panic(fmt.Sprintf("bkey too short, must have at least 14 bytes: '%v'", string(bkey)))
	}
	typ := bkey[0]
	if typ != '~' && typ != '>' {
		panic(fmt.Sprintf("bkey did not start with '~' for value nor '>' for symlink: '%v'", string(bkey)))
	}
	if bkey[n-10] != '<' {
		panic(fmt.Sprintf("bkey did not have '<' at 9 bytes from the end: '%v'", string(bkey)))
	}
	if bkey[n-1] != '#' {
		panic(fmt.Sprintf("bkey did not end in '#': '%v'", string(bkey)))
	}
}

// KeyExtractContainerKey extracts the containerKey from bkey.
// key example: field;view<ckey
// shortest: ~f;v<12345678#
//           1234567890123456789012345
//  numbering len(bkey) - i:
//           5432109876543210987654321
func KeyExtractContainerKey(bkey []byte) (containerKey uint64) {
	n := len(bkey)
	MustValidateKey(bkey)
	containerKey = binary.BigEndian.Uint64(bkey[(n - 9):(n - 1)])
	return
}

func AllShardPrefix(index, field, view string) (r []byte) {
	r = make([]byte, 0, 64)
	r = append(r, '~')
	r = append(r, []byte(field)...)
	r = append(r, ';')
	r = append(r, []byte(view)...)
	r = append(r, '<')
	return
}

// Prefix returns everything from Key up to and
// including the '<' byte in a Key. The prefix excludes the roaring container key itself.
// NB must be kept in sync with Key() and KeyExtractContainerKey().
func Prefix(index, field, view string, shard uint64) (r []byte) {
	r = make([]byte, 0, 32)
	r = append(r, '~')
	r = append(r, []byte(field)...)
	r = append(r, ';')
	r = append(r, []byte(view)...)
	r = append(r, '<')
	return
}

// IndexOnlyPrefix returns a "~" prefix suitable for DeleteIndex and a key-scan to
// remove all storage. We assume only one index in this database, so delete everything.
//
func IndexOnlyPrefix(indexName string) (r []byte) {
	return []byte("~")
}

// same for deleting a whole field.
func FieldPrefix(index, field string) (r []byte) {
	r = make([]byte, 0, 16)
	r = append(r, '~')
	r = append(r, []byte(field)...)
	r = append(r, ';')
	return
}

// PrefixFromKey key example: ~field;view<ckey#
//                  n-9    n-1
// ... : 01234567 < 01234567  #
//          view       ckey
func PrefixFromKey(bkey []byte) (prefix []byte) {
	n := len(bkey)
	return bkey[:(n - 9)]
}

func ToString(bkey []byte) (r string) {
	field, view, ckey := Split(bkey)
	return fmt.Sprintf("fld:'%v';vw:'%v';ckey@%020d", field, view, ckey)
}

func PrefixToString(pre []byte) (r string) {
	field, view := SplitPrefix(pre)
	return fmt.Sprintf("fld:'%v';vw:'%v';", field, view)
}

func Split(bkey []byte) (field, view string, ckey uint64) {
	ckey = KeyExtractContainerKey(bkey)
	n := len(bkey)
	field, view = SplitPrefix(bkey[:(n - 9)])
	return
}

// full key: ~field;view<ckey#
// prefix  : ~field;view<
func SplitPrefix(pre []byte) (field, view string) {
	n := len(pre)

	// prefix: ~field;view<
	beg := 1
	for i := 1; i < n; i++ {
		switch pre[i] {
		case ';':
			field = string(pre[beg:i])
			beg = i + 1
			view = string(pre[beg:(n - 1)])
			return
		}
	}
	panic(fmt.Sprintf("malformed prefix '%v' / '%#v', could not Split", string(pre), pre))
}
