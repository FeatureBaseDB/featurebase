// home https://github.com/glycerine/lmdb-go
// Copyright (c) 2020, the lmdb-go authors
// Copyright (c) 2015, Bryan Matsuo
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

//     Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.

//     Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.

//     Neither the name of the author nor the names of its contributors may be
//     used to endorse or promote products derived from this software without specific
//     prior written permission.

// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// +build amd64

package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/glycerine/lmdb-go/lmdb"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/hash"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/txkey"
)

// pilosa-keydump is a diagnostic tool that simply prints all the
// database keys in the database directory path specified
// as the first argument on the command line.
func main() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "must supply path to database directory as only arg\n")
		os.Exit(1)
	}

	path := os.Args[1]
	if !DirExists(path) {
		fmt.Fprintf(os.Stderr, "directory path '%v' does not exist.\n", path)
		os.Exit(1)
	}

	maxr := 1
	env, err := lmdb.NewEnvMaxReaders(maxr)
	panicOn(err)
	defer env.Close()

	panicOn(env.SetMapSize(256 << 30))

	err = env.SetMaxDBs(10)
	panicOn(err)

	//var myflags uint = NoReadahead | NoSubdir
	var myflags uint = 0 //lmdb.NoSubdir
	err = env.Open(path, myflags, 0664)
	panicOn(err)

	// In any real application it is important to check for readers that were
	// never closed by their owning process, and for which the owning process
	// has exited.  See the documentation on transactions for more information.
	staleReaders, err := env.ReaderCheck()
	panicOn(err)
	if staleReaders > 0 {
		vv("cleared %d reader slots from dead processes", staleReaders)
	}

	dbnames := []string{}

	var dbiRoot lmdb.DBI
	var dbi lmdb.DBI
	env.UseSphynxReader()
	err = env.SphynxReader(func(txn *lmdb.Txn, readslot int) (err error) {
		//txn.RawRead = true

		dbiRoot, err = txn.OpenRoot(0)
		panicOn(err)

		cur, err := txn.OpenCursor(dbiRoot)
		panicOn(err)
		defer cur.Close()

		for i := 0; true; i++ {
			var k, v []byte
			var err error
			if i == 0 {
				// must give it at least a zero byte here to start.
				k, v, err = cur.Get([]byte{0}, nil, lmdb.SetRange)
				panicOn(err)
			} else {
				k, v, err = cur.Get([]byte(nil), nil, lmdb.Next)
				if lmdb.IsNotFound(err) {
					break
				} else {
					panicOn(err)
				}
			}
			dbnames = append(dbnames, string(k))
			_ = v
		}
		cur.Close()
		return
	})
	panicOn(err)

	for _, dbn := range dbnames {
		fmt.Printf(`
=========================
database '%v':
=========================

`, dbn)
		err = env.SphynxReader(func(txn *lmdb.Txn, readslot int) (err error) {
			//txn.RawRead = true

			dbi, err = txn.OpenDBI(dbn, 0)
			panicOn(err)

			cur, err := txn.OpenCursor(dbi)
			panicOn(err)
			defer cur.Close()

			for i := 0; true; i++ {
				var k, v []byte
				var err error
				if i == 0 {
					// must give it at least a zero byte here to start.
					k, v, err = cur.Get([]byte{0}, nil, lmdb.SetRange)
					panicOn(err)
				} else {
					k, v, err = cur.Get([]byte(nil), nil, lmdb.Next)
					if lmdb.IsNotFound(err) {
						break
					} else {
						panicOn(err)
					}
				}

				ckey := txkey.KeyExtractContainerKey(k)

				n := len(v)

				hash := hash.Blake3sum16(v[0:(n - 1)])
				ct := pilosa.ToContainer(v[n-1], v[0:(n-1)])
				cts := roaring.NewSliceContainers()
				cts.Put(ckey, ct)
				rbm := &roaring.Bitmap{Containers: cts}
				srbm := pilosa.BitmapAsString(rbm)

				fmt.Printf("%04v  %v -> %v (%v hot)\n", i, txkey.ToString(k), hash, ct.N())
				fmt.Printf("          .......%v\n", srbm)
			}
			return
		})
		panicOn(err)
	} // for dbnames

	fmt.Printf("=================== done.\n")
}
