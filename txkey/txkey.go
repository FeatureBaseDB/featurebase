// Package txkey consolidates in one place the use of keys to index into our
// various storage/txn back-ends. Databases LMDB and rbfDB both use it,
// so that debug Dumps are comparable.
package txkey

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
	_, field, view, _ := SplitPrefix(prefix)
	return FieldView{Field: field, View: view}
}

func FieldViewFromFullKey(fullKey []byte) FieldView {
	_, field, view, _, _ := Split(fullKey)
	return FieldView{Field: field, View: view}
}

// Key produces the bytes that we use as a key to query the storage/tx engine.
// The roaringContainerKey argument to Key() is a container key into a roaring Container.
// The return value from Key() is constructed as follows:
//
//     ~index%field;view:shard<ckey#
//
// where shard and ckey are always exactly 8 bytes, uint64 big-endian encoded.
//
// Keys always start with either '~' or '>'. Keys always end with '#'.
// Keys always contain exactly one each of '%', ';', ':' and '<', in that order.
// The index is between the first byte and the '%'. It must be at least 1 byte long.
// The field is between the '%' and the ';'. It must be at least 1 byte long.
// The view is between the ';' and the ':'. It must be at least 1 byte long.
// The shard is the 8 bytes between the ':' and the '<'.
// The ckey is the 8 bytes between the '<' and the '#'.
// The Prefix of a key ends at, and includes, the '<'. It is at least 16 bytes long.
// The index, field, and view are not allowed to contain these reserved bytes:
//  {'~', '>', ';', ':', '<', '#', '$', '%', '^', '(', ')', '*', '!'}
//
// The bytes {'+', '/', '-', '_', '.', and '=' can be used in index, field, and view; to enable
// base-64 encoding.
//
// The shortest possible key is 25 bytes. It would be laid out like this:
//           ~i%f;v:12345678<12345678#
//           1234567890123456789012345
//
// keys starting with '~' are regular value keys.
// keys starting with '>' are symlink keys.
//
// NB must be kept in sync with Prefix() and KeyExtractContainerKey().
//
func Key(index, field, view string, shard uint64, roaringContainerKey uint64) (r []byte) {

	prefix := Prefix(index, field, view, shard)

	var ckey [9]byte
	binary.BigEndian.PutUint64(ckey[:8], roaringContainerKey)
	ckey[8] = byte('#')
	return append(prefix, ckey[:]...)
}

// ShardFromKey key example: index/field;view:shard<ckey
//                  n-9    n-1
// ... : 01234567 < 01234567   #
//          shard       ckey
func ShardFromKey(bkey []byte) (shard uint64) {
	MustValidateKey(bkey)
	n := len(bkey)
	// ckey is always exactly 8 bytes long
	// shard is always exactly 8 bytes long
	shard = binary.BigEndian.Uint64(bkey[(n - 18):(n - 10)])
	return
}

func ShardFromPrefix(prefix []byte) (shard uint64) {
	n := len(prefix)
	shard = binary.BigEndian.Uint64(prefix[(n - 9):(n - 1)])
	return
}

// KeyAndPrefix returns the equivalent of Key() and Prefix() calls.
func KeyAndPrefix(index, field, view string, shard uint64, roaringContainerKey uint64) (key, prefix []byte) {
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
	if n < 25 {
		panic(fmt.Sprintf("bkey too short, must have at least 25 bytes: '%v'", string(bkey)))
	}
	typ := bkey[0]
	if typ != '~' && typ != '>' {
		panic(fmt.Sprintf("bkey did not start with '~' for value nor '>' for symlink: '%v'", string(bkey)))
	}
	if bkey[n-10] != '<' {
		panic(fmt.Sprintf("bkey did not have '<' at 9 bytes from the end: '%v'", string(bkey)))
	}
	if bkey[n-19] != ':' {
		panic(fmt.Sprintf("bkey did not have '<' at 18 bytes from the end: '%v'", string(bkey)))
	}
	if bkey[n-1] != '#' {
		panic(fmt.Sprintf("bkey did not end in '#': '%v'", string(bkey)))
	}
}

// KeyExtractContainerKey extracts the containerKey from bkey.
// key example: index/field;view:shard<ckey
// shortest: =i%f;v:12345678<12345678#
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
	r = append(r, []byte(index)...)
	r = append(r, '%')
	r = append(r, []byte(field)...)
	r = append(r, ';')
	r = append(r, []byte(view)...)
	r = append(r, ':')
	return
}

// Prefix returns everything from Key up to and
// including the '<' byte in a Key. The prefix excludes the roaring container key itself.
// NB must be kept in sync with Key() and KeyExtractContainerKey().
func Prefix(index, field, view string, shard uint64) (r []byte) {
	r = make([]byte, 0, 32)
	r = append(r, '~')
	r = append(r, []byte(index)...)
	r = append(r, '%')
	r = append(r, []byte(field)...)
	r = append(r, ';')
	r = append(r, []byte(view)...)
	r = append(r, ':')

	var sh [8]byte
	binary.BigEndian.PutUint64(sh[:], shard)
	r = append(r, sh[:]...)
	r = append(r, '<')
	return
}

// IndexOnlyPrefix returns a prefix suitable for DeleteIndex and a key-scan to
// remove all storage associated with one index.
//
// The full name of the index must be provided, no partial index names will work.
//
// The returned prefix is terminated by '%' and so DeleteIndex("i") will not delete the index "i2".
//
func IndexOnlyPrefix(indexName string) (r []byte) {
	r = make([]byte, 0, 32)
	r = append(r, '~')
	r = append(r, []byte(indexName)...)
	r = append(r, '%')
	return
}

// same for deleting a whole field.
func FieldPrefix(index, field string) (r []byte) {
	r = make([]byte, 0, 32)
	r = append(r, '~')
	r = append(r, []byte(index)...)
	r = append(r, '%')
	r = append(r, []byte(field)...)
	r = append(r, ';')
	return
}

// PrefixFromKey key example: index/field;view:shard<ckey
//                  n-9    n-1
// ... : 01234567 < 01234567  #
//          shard       ckey
func PrefixFromKey(bkey []byte) (prefix []byte) {
	n := len(bkey)
	return bkey[:(n - 9)]
}

func ToString(bkey []byte) (r string) {
	index, field, view, shard, ckey := Split(bkey)
	return fmt.Sprintf("idx:'%v';fld:'%v';vw:'%v';shd:'%020v';ckey@%020d", index, field, view, shard, ckey)
}

func PrefixToString(pre []byte) (r string) {
	index, field, view, shard := SplitPrefix(pre)
	return fmt.Sprintf("idx:'%v';fld:'%v';vw:'%v';shd:'%020v';", index, field, view, shard)
}

func Split(bkey []byte) (index, field, view string, shard, ckey uint64) {
	ckey = KeyExtractContainerKey(bkey)
	n := len(bkey)
	index, field, view, shard = SplitPrefix(bkey[:(n - 9)])
	return
}

// full key: ~index%field;view:shard<ckey#
// prefix  : ~index%field;view:shard<
func SplitPrefix(pre []byte) (index, field, view string, shard uint64) {
	n := len(pre)
	shard = binary.BigEndian.Uint64(pre[(n - 9):(n - 1)])

	// prefix: =index%field;view:shard<
	goal := byte('%')
	beg := 1
	for i := 1; i < n; i++ {
		c := pre[i]
		switch goal {
		case '%':
			if c == goal {
				index = string(pre[beg:i])
				beg = i + 1
				goal = byte(';')
			}
		case ';':
			if c == goal {
				field = string(pre[beg:i])
				beg = i + 1
				view = string(pre[beg:(n - 10)])
				return
			}
		}
	}
	panic(fmt.Sprintf("malformed prefix '%v' / '%#v', could not Split", string(pre), pre))
}
