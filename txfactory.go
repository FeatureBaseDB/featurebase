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
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/txkey"
	"github.com/pkg/errors"
	"github.com/zeebo/blake3"
)

// public strings that pilosa/server/config.go can reference
const (
	RoaringTxn string = "roaring"
	BadgerTxn  string = "badger"
	LmdbTxn    string = "lmdb"
	RBFTxn     string = "rbf"
)

// DefaultTxsrc is set here. pilosa/server/config.go references it
// to set the default for pilosa server exeutable.
// Can be overridden with env variable PILOSA_TXSRC for testing.
const DefaultTxsrc = RoaringTxn

// DetectMemAccessPastTx true helps us catch places in api and executor
// where mmapped memory is being accessed after the point in time
// which the transaction has committed or rolled back. Since
// memory segments will be recycled by the underlying databases,
// this can lead to corruption. When DetectMemAccessPastTx is true,
// code in badger.go will copy the transactionally viewed memory before
// returning it for bitmap reading, and then zero it or overwrite it
// with -2 when the Tx completes.
//
// Should be false for production.
//
const DetectMemAccessPastTx = false

var sep = string(os.PathSeparator)

// TxFactory abstracts the creation of Tx interface-level
// transactions so that RBF, or Badger, or Roaring-fragment-files, or several
// of these at once in parallel, is used as the storage and transction layer.
type TxFactory struct {
	typeOfTx string

	types []txtype // blue-green split individually here

	badgerDB  *BadgerDBWrapper
	lmDB      *LMDBWrapper
	rbfDB     *RbfDBWrapper
	roaringDB *RoaringStore

	dbsClosed bool // idemopotent CloseDB()

	// could have more than one *Index, but for now keep it simple,
	// and allow blueGreenTx to report badger contents via idx
	idx *Index
}

// integer types for fast switch{}
type txtype int

const (
	noneTxn    txtype = 0
	roaringTxn txtype = 1 // these don't really have any transactions
	badgerTxn  txtype = 2
	rbfTxn     txtype = 3
	lmdbTxn    txtype = 4
)

func (txf *TxFactory) NeedsSnapshot() (b bool) {
	for _, ty := range txf.types {
		switch ty {
		case roaringTxn:
			b = true
		}
	}
	return
}

func MustTxsrcToTxtype(txsrc string) (types []txtype) {

	var srcs []string
	if strings.Contains(txsrc, "_") {
		srcs = strings.Split(txsrc, "_")
		if len(srcs) != 2 {
			panic("only two blue-green comparisons permitted")
		}
	} else {
		srcs = append(srcs, txsrc)
	}

	for i, s := range srcs {
		switch s {
		case RoaringTxn: // "roaring"
			types = append(types, roaringTxn)
		case BadgerTxn: // "badger"
			types = append(types, badgerTxn)
		case RBFTxn: // "rbf"
			types = append(types, rbfTxn)
		case LmdbTxn: // "lmdb"
			types = append(types, lmdbTxn)
		default:
			panic(fmt.Sprintf("unknown txsrc '%v'", s))
		}
		if i == 1 {
			if types[1] == types[0] {
				panic(fmt.Sprintf("cannot blue-green the same txsrc on both arms: '%v'", s))
			}
		}
	}
	return
}

// NewTxFactory always opens an existing database. If you
// want to a fresh database, os.RemoveAll on dir/name ahead of time.
// We always store files in a subdir of dir. If we are having one
// database or many can depend on name.
func NewTxFactory(txsrc string, dir, name string) (f *TxFactory, err error) {
	//vv("NewTxFactory called for txsrc '%v'; dir='%v'; name='%v'", txsrc, dir, name)

	types := MustTxsrcToTxtype(txsrc)

	f = &TxFactory{
		types:     types,
		typeOfTx:  txsrc,
		roaringDB: NewRoaringStore(),
	}

	for _, ty := range f.types {
		switch ty {
		case roaringTxn:
			// no-op. these are just files in a directory

		case badgerTxn:
			// one, big, bad-ass badger for all data: the honeyBadger.
			//
			// Note that having a single Tx backing store for all indexes
			// enables cross-index Tx, which are important and are tested for.
			path := dir + sep + "honeyBadger"

			f.badgerDB, err = globalBadgerReg.openBadgerDBWrapper(path)
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("cannot open badger db. path='%v'", path))
			}

			// electric-fence like finding of access to mmapped data beyond
			// transaction end time.
			f.badgerDB.doAllocZero = DetectMemAccessPastTx

		case rbfTxn:
			path := dir + sep + "all-in-one-rbfdb"
			f.rbfDB, err = globalRbfDBReg.openRbfDB(path)
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("cannot create new rbf db. path='%v'", path))
			}
		case lmdbTxn:
			path := dir + sep + "all-in-one"
			f.lmDB, err = globalLMDBReg.openLMDBWrapper(path)
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("cannot create new lmdb db. path='%v'", path))
			}
		}
	}

	return f, err
}

// Txo holds the transaction options
type Txo struct {
	Write    bool
	Field    *Field
	Index    *Index
	Fragment *fragment
	Shard    uint64
}

func (f *TxFactory) TxType() string {
	return f.typeOfTx
}

func (f *TxFactory) TxTypes() []txtype {
	return f.types
}

func (f *TxFactory) DeleteIndex(name string) (err error) {
	for _, ty := range f.types {
		switch ty {
		case roaringTxn:
			// from holder.go:955, by default is already done there with os.RemoveAll()
		case badgerTxn:
			err = f.badgerDB.DeleteIndex(name)
		case rbfTxn:
			err = f.rbfDB.DeleteIndex(name)
		case lmdbTxn:
			err = f.lmDB.DeleteIndex(name)
		default:
			panic(fmt.Sprintf("unknown txtyp : '%v'", ty))
		}
	}
	return
}

func (f *TxFactory) DeleteFieldFromStore(index, field, fieldPath string) (err error) {
	for _, ty := range f.types {
		switch ty {
		case roaringTxn:
			err = f.roaringDB.DeleteField(index, field, fieldPath)
		case badgerTxn:
			err = f.badgerDB.DeleteField(index, field, fieldPath)
		case rbfTxn:
			err = f.rbfDB.DeleteField(index, field, fieldPath)
		case lmdbTxn:
			err = f.lmDB.DeleteField(index, field, fieldPath)
		default:
			panic(fmt.Sprintf("unknown f.typeOfTx type: '%v'", ty))
		}
	}
	return
}

func (f *TxFactory) DeleteFragmentFromStore(
	index, field, view string, shard uint64, frag *fragment,
) (err error) {

	for _, ty := range f.types {
		switch ty {
		case roaringTxn:
			err = f.roaringDB.DeleteFragment(index, field, view, shard, frag)
		case badgerTxn:
			err = f.badgerDB.DeleteFragment(index, field, view, shard, frag)
		case rbfTxn:
			err = f.rbfDB.DeleteFragment(index, field, view, shard, frag)
		case lmdbTxn:
			err = f.lmDB.DeleteFragment(index, field, view, shard, frag)
		default:
			panic(fmt.Sprintf("unknown f.typeOfTx type: '%v'", ty))
		}
	}
	return
}

func (f *TxFactory) CloseIndex(idx *Index) error {
	// under roaring and all the new databases, this is a no-op.
	//idx.Dump("CloseIndex")

	return nil
}

func (f *TxFactory) CloseDB() (err error) {
	if f.dbsClosed {
		return nil
	}
	f.dbsClosed = true

	for _, ty := range f.types {
		switch ty {
		case roaringTxn:
			// no-op
		case badgerTxn:
			err = f.badgerDB.Close()
		case rbfTxn:
			err = f.rbfDB.Close()
		case lmdbTxn:
			err = f.lmDB.Close()
		default:
			panic(fmt.Sprintf("unknown txtype: '%v'", ty))
		}
	}
	return
}

var globalUseStatTx = false

func init() {
	v := os.Getenv("PILOSA_CALLSTAT")
	if v != "" {
		globalUseStatTx = true
	}
}

func (f *TxFactory) NewTx(o Txo) (txn Tx) {
	defer func() {
		if globalUseStatTx {
			txn = newStatTx(txn)
		}
	}()
	indexName := ""
	if o.Index != nil {
		indexName = o.Index.name
	}
	var txns []Tx

	for _, ty := range f.types {
		switch ty {
		case roaringTxn:
			txns = append(txns, &RoaringTx{write: o.Write, Field: o.Field, Index: o.Index, fragment: o.Fragment})
		case badgerTxn:
			//tx := NewMultiTxWithIndex(o.Write, o.Index)
			//txns = append(txns,  tx
			btx := f.badgerDB.NewBadgerTx(o.Write, indexName, o.Fragment)
			txns = append(txns, btx)
		case rbfTxn:
			//tx := NewMultiTxWithIndex(o.Write, o.Index)
			tx, err := f.rbfDB.NewRBFTx(o.Write, indexName, o.Fragment)
			if err != nil {
				panic(errors.Wrap(err, "rbfDB.NewRBFTx transaction errored"))
			}
			txns = append(txns, tx)
		case lmdbTxn:
			txns = append(txns, f.lmDB.NewLMDBTx(o.Write, indexName, o.Fragment))
		default:
			panic(fmt.Sprintf("unknown txtyp: '%v'", ty))
		}
	}
	if len(txns) > 1 {
		return newBlueGreenTx(txns[0], txns[1], f.idx)
	}
	return txns[0]
}

func (ty txtype) String() string {
	switch ty {
	case noneTxn:
		return "noneTxn"
	case roaringTxn:
		return "roaringTxn"
	case badgerTxn:
		return "badgerTxn"
	case rbfTxn:
		return "rbfTxn"
	case lmdbTxn:
		return "lmdbTxn"
	}
	panic(fmt.Sprintf("unhandled ty '%v' in txtype.String()", int(ty)))
}

// StringifiedBadgerKeys displays the keys visible in BadgerDB for the idx *Index.
// If optionalUseThisTx is nil, it will start a new read-only transaction to
// do this query. Otherwise it will piggy back on the provided transaction.
// Hence to view uncommited keys, you must provide in optionalUseThisTx the
// Tx in which they have been added.
func (idx *Index) StringifiedBadgerKeys(optionalUseThisTx Tx) string {
	return idx.Txf.badgerDB.StringifiedBadgerKeys(optionalUseThisTx)
}

// fragmentSpecFromRoaringPath takes a path releative to the
// index directory, not including the name of the index itself.
// The path should not start with the path separator sep ('/' or '\\') rune.
func fragmentSpecFromRoaringPath(path string) (field, view string, shard uint64, err error) {
	if len(path) == 0 {
		err = fmt.Errorf("fragmentSpecFromRoaringPath error: path '%v' too short", path)
		return
	}
	if path[:1] == sep {
		err = fmt.Errorf("fragmentSpecFromRoaringPath error: path '%v' cannot start with separator '%v'; must be relative to the index base directory", path, sep)
		return
	}

	// sample path:
	// field         view               shard
	// myfield/views/standard/fragments/0
	s := strings.Split(path, "/")
	n := len(s)
	if n != 5 {
		err = fmt.Errorf("len(s)=%v, but expected 5. path='%v'", n, path)
		return
	}
	field = s[0]
	view = s[2]
	shard, err = strconv.ParseUint(s[4], 10, 64)
	if err != nil {
		err = fmt.Errorf("fragmentSpecFromRoaringPath(path='%v') could not parse shard '%v' as uint: '%v'", path, s[4], err)
	}
	return
}

// hashOnly means only show the value hash, not the content bits.
// showOps means display the ops log.
func (idx *Index) StringifiedRoaringKeys(hashOnly, showOps bool) (r string) {
	paths, err := listFilesUnderDir(idx.path, false, "", true)
	panicOn(err)
	index := idx.name

	r = "allkeys:[\n"
	n := 0
	for _, relpath := range paths {
		field, view, shard, err := fragmentSpecFromRoaringPath(relpath)
		if err != nil {
			continue // ignore .meta paths
		}
		abspath := idx.path + sep + relpath

		s, _, err := stringifiedRawRoaringFragment(abspath, index, field, view, shard, showOps, hashOnly, os.Stdout)
		panicOn(err)
		//r += fmt.Sprintf("path:'%v' fragment contains:\n") + s
		if s == "" {
			s = "<empty bitmap>"
		}
		r += s
		n++
	}
	if n == 0 {
		return "" // new convention that empty database => empty string returned.
	}
	// note that we can have a bitmap present, but it can be empty
	r += "]\n   all-in-blake3:" + blake3sum16([]byte(r)) + "\n"

	return "roaring-" + r
}

func RoaringFragmentChecksum(path string, index, field, view string, shard uint64) (r string, hotbits int) {
	hasher := blake3.New()
	showOps := false
	hashOnly := true
	_, hotbits, err := stringifiedRawRoaringFragment(path, index, field, view, shard, showOps, hashOnly, hasher)
	panicOn(err)
	fmt.Fprintf(hasher, "%v/%v/%v/%v", index, field, view, shard)
	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])
	return fmt.Sprintf("%x", buf), hotbits

}

func stringifiedRawRoaringFragment(path string, index, field, view string, shard uint64, showOps, hashOnly bool, w io.Writer) (r string, hotbits int, err error) {

	var info roaring.BitmapInfo
	_ = info
	var f *os.File
	f, err = os.Open(path)
	panicOn(err)
	if err != nil {
		return
	}

	var fi os.FileInfo
	fi, err = f.Stat()
	panicOn(err)
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
		err := syscall.Munmap(data)
		if err != nil {
			panic(fmt.Errorf("loadRawRoaringContainer: munmap failed: %v", err))
		}
		panicOn(f.Close())
	}()

	// Attach the mmap file to the bitmap.
	var rbm *roaring.Bitmap
	rbm, _, err = roaring.InspectBinary(data, true, &info)
	if err != nil {
		err = errors.Wrap(err, "inspecting")
		return
	}

	//cmd.DisplayInfo(info)
	// inlined
	if showOps {
		pC := pointerContext{
			from: info.From,
			to:   info.To,
		}
		if info.ContainerCount > 0 {
			printContainers(w, info, pC)
		}
		if info.Ops > 0 {
			printOps(w, info)
		}
	}

	citer, found := rbm.Containers.Iterator(0)
	_ = found // probably gonna use just the Ops log instead, so don't panic if !found.

	for citer.Next() {
		ckey, ct := citer.Value()
		by := containerToBytes(ct)
		hash := blake3sum16(by)

		cts := roaring.NewSliceContainers()
		cts.Put(ckey, ct)
		rbm := &roaring.Bitmap{Containers: cts}
		var srbm string
		if !hashOnly {
			srbm = bitmapAsString(rbm)
		}

		bkey := string(txkey.Key(index, field, view, shard, ckey))

		n := ct.N()
		hotbits += int(n)
		r += fmt.Sprintf("%v -> %v (%v hot)\n", bkey, hash, n)
		if !hashOnly {
			r += "          ......." + srbm + "\n"
		}
	}

	return
}

// listFilesUnderDir returns the paths of files found under directory root.
// If includeRoot is true, it returns the full path, otherwise paths are relative to root.
// If requriedSuffix is supplied, the returned file paths will end in that,
// and any other files found during the walk of the directory tree will be ignored.
// If ignoreEmpty is true, files of size 0 will be excluded.
func listFilesUnderDir(root string, includeRoot bool, requiredSuffix string, ignoreEmpty bool) (files []string, err error) {
	if !dirExists(root) {
		return nil, fmt.Errorf("listFilesUnderDir error: root directory '%v' not found", root)
	}
	n := len(root) + 1
	if includeRoot {
		n = 0
	}
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if len(path) < n {
			// ignore
		} else {
			if info == nil {
				panic(fmt.Sprintf("info was nil for path = '%v'", path))
			}
			if info.IsDir() {
				// skip directories.
			} else {
				if ignoreEmpty && info.Size() == 0 {
					return nil
				}
				if requiredSuffix == "" || strings.HasSuffix(path, requiredSuffix) {
					files = append(files, path[n:])
				}
			}
		}
		return nil
	})
	return
}

func dirExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

func fileSize(name string) (int64, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return -1, err
	}
	return fi.Size(), nil
}

var _ = fileSize // happy linter

func containerToBytes(ct *roaring.Container) []byte {
	ty := roaring.ContainerType(ct)
	switch ty {
	case roaring.ContainerNil:
		panic("nil roaring.Container")
	case roaring.ContainerArray:
		return fromArray16(roaring.AsArray(ct))
	case roaring.ContainerBitmap:
		return fromArray64(roaring.AsBitmap(ct))
	case roaring.ContainerRun:
		return fromInterval16(roaring.AsRuns(ct))
	}
	panic(fmt.Sprintf("unknown roaring.Container type '%v'", int(ty)))
}

type pointerContext struct {
	from, to uintptr
}

func printOps(w io.Writer, info roaring.BitmapInfo) {
	fmt.Fprintln(w, "  Ops:")
	tw := tabwriter.NewWriter(w, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "  \t%s\t%s\t%s\t\n", "TYPE", "OpN", "SIZE")
	printed := 0
	for _, op := range info.OpDetails {
		fmt.Fprintf(tw, "\t%s\t%d\t%d\t\n", op.Type, op.OpN, op.Size)
		printed++
	}
	tw.Flush()
}

func (p *pointerContext) pretty(c roaring.ContainerInfo) string {
	var pointer string
	if c.Mapped {
		if c.Pointer >= p.from && c.Pointer < p.to {
			pointer = fmt.Sprintf("@+0x%x", c.Pointer-p.from)
		} else {
			pointer = fmt.Sprintf("!0x%x!", c.Pointer)
		}
	} else {
		pointer = fmt.Sprintf("0x%x", c.Pointer)
	}
	return fmt.Sprintf("%s \t%d \t%d \t%s ", c.Type, c.N, c.Alloc, pointer)
}

// stolen from ctl/inspect.go
func printContainers(w io.Writer, info roaring.BitmapInfo, pC pointerContext) {
	fmt.Fprintln(w, "  Containers:")
	tw := tabwriter.NewWriter(w, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "  \t\tRoaring\t\t\t\tOps\t\t\t\tFlags\t\n")
	fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n", "KEY", "TYPE", "N", "ALLOC", "OFFSET", "TYPE", "N", "ALLOC", "OFFSET", "FLAGS")
	c1s := info.Containers
	c2s := info.OpContainers
	l1 := len(c1s)
	l2 := len(c2s)
	i1 := 0
	i2 := 0
	var c1, c2 roaring.ContainerInfo
	c1.Key = ^uint64(0)
	c2.Key = ^uint64(0)
	c1e := false
	c2e := false
	if i1 < l1 {
		c1 = c1s[i1]
		i1++
		c1e = true
	}
	if i2 < l2 {
		c2 = c2s[i2]
		i2++
		c2e = true
	}
	printed := 0
	for c1e || c2e {
		c1used := false
		c2used := false
		var key uint64
		c1fmt := "-\t\t\t"
		c2fmt := "-\t\t\t"
		// If c2 exists, we'll always prefer its flags,
		// if it doesn't, this gets overwritten.
		flags := c2.Flags
		if !c2e || (c1e && c1.Key < c2.Key) {
			c1fmt = pC.pretty(c1)
			key = c1.Key
			c1used = true
			flags = c1.Flags
		} else if !c1e || (c2e && c2.Key < c1.Key) {
			c2fmt = pC.pretty(c2)
			key = c2.Key
			c2used = true
		} else {
			// c1e and c2e both set, and neither key is < the other.
			c1fmt = pC.pretty(c1)
			c2fmt = pC.pretty(c2)
			key = c1.Key
			c1used = true
			c2used = true
		}
		if c1used {
			if i1 < l1 {
				c1 = c1s[i1]
				i1++
			} else {
				c1e = false
			}
		}
		if c2used {
			if i2 < l2 {
				c2 = c2s[i2]
				i2++
			} else {
				c2e = false
			}
		}
		fmt.Fprintf(tw, "\t%d\t%s\t%s\t%s\t\n", key, c1fmt, c2fmt, flags)
		printed++
	}
	tw.Flush()
}
