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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

// public strings that pilosa/server/config.go can reference
const (
	RoaringTxn string = "roaring"
	BadgerTxn  string = "badger"
	RBFTxn     string = "rbf"
	// A is listed first, B is second. blueGreenTx returns the B output.
	BlueGreenBadgerRoaring string = "badger_roaring"
	BlueGreenRoaringBadger string = "roaring_badger"

	BlueGreenRBFRoaring string = "rbf_roaring"
	BlueGreenRoaringRBF string = "roaring_rbf"

	BlueGreenBadgerRBF string = "badger_rbf"
	BlueGreenRBFBadger string = "rbf_badger"
)

// DefaultTxsrc is set here. pilosa/server/config.go references it
// to set the default for pilosa server exeutable.
// Can be overridden with env variable PILOSA_TXSRC for testing.
const DefaultTxsrc = RoaringTxn

var sep = string(os.PathSeparator)

// TxFactory abstracts the creation of Tx interface-level
// transactions so that RBF, or Badger, or Roaring-fragment-files, or several
// of these at once in parallel, is used as the storage and transction layer.
type TxFactory struct {
	typeOfTx txtype

	bw *BadgerDBWrapper

	// could have more than one *Index, but for now keep it simple,
	// and allow blueGreenTx to report badger contents via idx
	idx *Index

	// TODO:  put RBF database handle here.
}

// integer types for fast switch{}
type txtype int

const (
	noneTxn txtype = 0

	roaringFragmentFilesTxn txtype = 1 // these don't really have any transactions
	badgerTxn               txtype = 2
	rbfTxn                  txtype = 3

	// A is listed first, B is second. blueGreenTx returns the B output.
	blueGreenBadgerRoaring txtype = 4
	blueGreenRoaringBadger txtype = 5

	blueGreenRBFRoaring txtype = 6
	blueGreenRoaringRBF txtype = 7

	blueGreenBadgerRBF txtype = 8
	blueGreenRBFBadger txtype = 9
)

func txsrcToTxtype(txsrc string) txtype {
	switch txsrc {
	case RoaringTxn: // "roaring"
		return roaringFragmentFilesTxn
	case BadgerTxn: // "badger"
		return badgerTxn
	case RBFTxn: // "rbf"
		return rbfTxn
	case BlueGreenBadgerRoaring: //"badger_roaring"
		return blueGreenBadgerRoaring
	case BlueGreenRoaringBadger: // "roaring_badger"
		return blueGreenRoaringBadger
	case BlueGreenRBFRoaring: //"rbf_roaring"
		return blueGreenRBFRoaring
	case BlueGreenRoaringRBF: //"roaring_rbf"
		return blueGreenRoaringRBF
	case BlueGreenBadgerRBF: // "badger_rbf"
		return blueGreenBadgerRBF
	case BlueGreenRBFBadger: //  "rbf_badger"
		return blueGreenRBFBadger
	}
	panic(fmt.Sprintf("unknown txsrc '%v'", txsrc))
}

func newTxFactory(txsrc string, path string) (f *TxFactory, err error) {
	ty := txsrcToTxtype(txsrc)
	if ty < 1 || ty > 9 {
		panic(fmt.Sprintf("invalid txtype '%v'", int(ty)))
	}
	var bw *BadgerDBWrapper
	if ty == badgerTxn || ty == 4 || ty == 5 || ty == 8 || ty == 9 {
		bw, err = openBadgerDBWrapper(path)
		// TODO(jea): figure out what the appropriate error path is here.
		//fmt.Printf("warning: could not open badgerdb on path '%v': '%v'. For safety, we are opening a new '%v-fallback' instead\n", path, err, path+"-fallback")
		if err != nil {
			//bw, err = newBadgerDBWrapper(path + "-fallback")
			bw, err = newBadgerDBWrapper(path)
		}
		panicOn(err)

		bw.doAllocZero = true
	}
	return &TxFactory{
		typeOfTx: ty,
		bw:       bw,
	}, err
}

// Txo holds the transaction options
type Txo struct {
	Write    bool
	Field    *Field
	Index    *Index
	Fragment *fragment
	Shard    uint64
}

func (f *TxFactory) TxType() txtype {
	return f.typeOfTx
}

func (f *TxFactory) DeleteIndex(name string) error {
	switch f.typeOfTx {
	case roaringFragmentFilesTxn:
		// from holder.go:955, by default is already done there with os.RemoveAll()
		return nil
	case badgerTxn:
		return f.bw.DeleteIndex(name)
	case rbfTxn:
		panic("todo rbfTxn DeleteIndex(name)")
	case blueGreenBadgerRoaring:
		return f.bw.DeleteIndex(name)
	case blueGreenRoaringBadger:
		return f.bw.DeleteIndex(name)
	}
	panic(fmt.Sprintf("unknown f.typeOfTx type: '%v'", f.typeOfTx))
}

func (f *TxFactory) Close() error {
	switch f.typeOfTx {
	case roaringFragmentFilesTxn:
		return nil
	case badgerTxn:
		// note cannot actually close Badger here.
		// causes problems b/c tries holder.DeleteIndex tries to delete the index after db is closed.
		//return f.bw.Close()
		return nil
	case rbfTxn:
		panic("todo rbfTxn Close()")
	case blueGreenBadgerRoaring:
		return nil
	case blueGreenRoaringBadger:
		return nil
	}
	panic(fmt.Sprintf("unknown f.typeOfTx type: '%v'", f.typeOfTx))
}

func (f *TxFactory) CloseIndex(idx *Index) error {
	switch f.typeOfTx {
	case roaringFragmentFilesTxn:
		return nil
	case badgerTxn:
		return nil
	case rbfTxn:
		panic("todo rbfTxn CloseIndex()")

	case blueGreenBadgerRoaring:
		return nil
	case blueGreenRoaringBadger:
		return nil
	}
	panic(fmt.Sprintf("unknown f.typeOfTx type: '%v'", f.typeOfTx))
}

func (f *TxFactory) NewTx(o Txo) Tx {

	switch f.typeOfTx {
	case roaringFragmentFilesTxn:
		return &RoaringTx{write: o.Write, Field: o.Field, Index: o.Index, fragment: o.Fragment}
	case badgerTxn:
		btx := f.bw.NewBadgerTx(o.Write)
		return btx
	case rbfTxn:
		panic("todo rbfTxn creation")

	case blueGreenBadgerRoaring:
		btx := f.bw.NewBadgerTx(o.Write)
		rtx := &RoaringTx{write: o.Write, Field: o.Field, Index: o.Index, fragment: o.Fragment}
		return newBlueGreenTx(btx, rtx, f.idx)
	case blueGreenRoaringBadger:
		btx := f.bw.NewBadgerTx(o.Write)
		rtx := &RoaringTx{write: o.Write, Field: o.Field, Index: o.Index, fragment: o.Fragment}
		return newBlueGreenTx(rtx, btx, f.idx)
	}
	panic(fmt.Sprintf("unknown f.typeOfTx type: '%v'", f.typeOfTx))
}

func (ty txtype) String() string {
	switch ty {
	case noneTxn:
		return "noneTxn"
	case roaringFragmentFilesTxn:
		return "roaringFragmentFilesTxn"
	case badgerTxn:
		return "badgerTxn"
	case rbfTxn:
		return "rbfTxn"
	case blueGreenBadgerRoaring:
		return "blueGreenBadgerRoaring"
	case blueGreenRoaringBadger:
		return "blueGreenRoaringBadger"
	case blueGreenRBFRoaring:
		return "blueGreenRBFRoaring"
	case blueGreenRoaringRBF:
		return "blueGreenRoaringRBF"
	case blueGreenBadgerRBF:
		return "blueGreenBadgerRBF"
	case blueGreenRBFBadger:
		return "blueGreenRBFBadger"
	}
	panic(fmt.Sprintf("unhandled ty '%v' in txtype.String()", int(ty)))
}

// StringifiedBadgerKeys displays the keys visible in BadgerDB for the idx *Index.
// If optionalUseThisTx is nil, it will start a new read-only transaction to
// do this query. Otherwise it will piggy back on the provided transaction.
// Hence to view uncommited keys, you must provide in optionalUseThisTx the
// Tx in which they have been added.
func (idx *Index) StringifiedBadgerKeys(optionalUseThisTx Tx) string {
	return idx.Txf.bw.StringifiedBadgerKeys(optionalUseThisTx)
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

func (idx *Index) StringifiedRoaringKeys() (r string) {

	paths, err := listFilesUnderDir(idx.path, false, "", true)
	panicOn(err)
	index := idx.name

	r = "allkeys:[\n"
	for _, relpath := range paths {
		field, view, shard, err := fragmentSpecFromRoaringPath(relpath)
		if err != nil {
			continue // ignore .meta paths
		}
		abspath := idx.path + sep + relpath
		s, err := stringifiedRawRoaringFragment(abspath, index, field, view, shard)
		panicOn(err)
		//r += fmt.Sprintf("path:'%v' fragment contains:\n") + s
		r += s
	}
	r += "]\n   all-in-blake3:" + blake3sum16([]byte(r)) + "\n"

	return "roaring-" + r
}

func stringifiedRawRoaringFragment(path string, index, field, view string, shard uint64) (r string, err error) {

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

	citer, found := rbm.Containers.Iterator(0)
	_ = found // probably gonna use just the Ops log instead, so don't panic if !found.

	for citer.Next() {
		ckey, ct := citer.Value()
		by := containerToBytes(ct)
		hash := blake3sum16(by)

		cts := roaring.NewSliceContainers()
		cts.Put(ckey, ct)
		rbm := &roaring.Bitmap{Containers: cts}
		srbm := bitmapAsString(rbm)
		panicOn(err)

		bkey := string(badgerKey(index, field, view, shard, ckey))

		r += fmt.Sprintf("%v -> %v (%v hot)\n", bkey, hash, ct.N())
		r += "          ......." + srbm + "\n"
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

// Dump prints to stdout the contents of the roaring Containers
// stored in idx. Its format may vary depending of the type of
// idx.Txf transaction factory that is in use.
// Mostly for debugging.
func (idx *Index) Dump(label string) {
	ty := idx.Txf.TxType()
	fileline := FileLine(2)
	switch ty {
	case badgerTxn:
		fmt.Printf("%v Index.Dump('%v') for index '%v':\n%v\n", fileline, label, idx.name, idx.StringifiedBadgerKeys(nil))
		return
	case blueGreenRoaringBadger, blueGreenBadgerRoaring:
		fmt.Printf("%v Index.Dump('%v') for index '%v', RoaringTx:\n%v\n", fileline, label, idx.name, idx.StringifiedRoaringKeys())
		fmt.Printf("%v Index.Dump('%v') for index '%v', BadgerTx :\n%v\n", fileline, label, idx.name, idx.StringifiedBadgerKeys(nil))
		return
	case roaringFragmentFilesTxn:
		fmt.Printf("%v Index.Dump('%v') for index '%v', BadgerTx :\n%v\n", fileline, label, idx.name, idx.StringifiedRoaringKeys())
		return
	}
	panic(fmt.Errorf("%v Index.Dump('%v') for index '%v': no implementation for txtype '%v'\n", fileline, label, idx.name, ty))
}

func containerToBytes(ct *roaring.Container) []byte {
	ty := roaring.ContainerType(ct)
	switch ty {
	case containerNil:
		panic("nil container")
	case containerArray:
		return fromArray16(roaring.AsArray(ct))
	case containerBitmap:
		return fromArray64(roaring.AsBitmap(ct))
	case containerRun:
		return fromInterval16(roaring.AsRuns(ct))
	}
	panic(fmt.Sprintf("unknown container type '%v'", int(ty)))
}
