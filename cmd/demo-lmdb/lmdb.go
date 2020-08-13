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

// +build !386
// +build skip_building_lmdb_for_now

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/glycerine/lmdb-go/lmdb"
)

// note: use runtime.LockOSThread on any write goroutine; must create and write the txn from
// the same goroutine.

// This example demonstrates a complete workflow for a simple application
// working with LMDB.  First, an Env is configured and mapped to memory.  Once
// mapped, database handles are opened and normal database operations may
// begin.
func main() {
	// Create an environment and make sure it is eventually closed.
	env, err := lmdb.NewEnv()
	panicOn(err)
	defer env.Close()

	// Configure and open the environment.  Most configuration must be done
	// before opening the environment.  The go documentation for each method
	// should indicate if it must be called before calling env.Open()
	err = env.SetMaxDBs(1)
	panicOn(err)
	err = env.SetMapSize(1 << 30)
	panicOn(err)
	path := "./db-lmdb"
	panicOn(os.MkdirAll(path, 0755))
	err = env.Open(path, 0, 0644) // lmdb.Create ?
	panicOn(err)

	// In any real application it is important to check for readers that were
	// never closed by their owning process, and for which the owning process
	// has exited.  See the documentation on transactions for more information.
	staleReaders, err := env.ReaderCheck()
	panicOn(err)
	if staleReaders > 0 {
		log.Printf("cleared %d reader slots from dead processes", staleReaders)
	}

	// Open a database handle that will be used for the entire lifetime of this
	// application.  Because the database may not have existed before, and the
	// database may need to be created, we need to get the database handle in
	// an update transacation.
	var dbi lmdb.DBI
	_ = dbi
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("example")
		return err
	})
	panicOn(err)

	// The database referenced by our DBI handle is now ready for the
	// application to use.  Here the application just opens a readonly
	// transaction and reads the data stored in the "hello" key and prints its
	// value to the application's standard output.
	err = env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte("hello"))
		if err != nil {
			return err
		}
		fmt.Println(string(v))
		return nil
	})
	_ = err
	//panicOn(err) // mdb_get: MDB_NOTFOUND: No matching key/data pair found

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		panicOn(txn.Put(dbi, []byte("099"), []byte("A"), 0))
		panicOn(txn.Put(dbi, []byte("101"), []byte("B"), 0))
		panicOn(txn.Put(dbi, []byte("199"), []byte("C"), 0))
		panicOn(txn.Put(dbi, []byte("200"), []byte("D"), 0))
		panicOn(txn.Put(dbi, []byte("300"), []byte("E"), 0))
		panicOn(txn.Put(dbi, []byte("399"), []byte("F"), 0))
		return nil
	})
	_ = err

	// find max in [000,100) and get 099
	// find max in [100,200) and get 199
	// find max in [300,400) and get 399
	// find max in [400,500) and get nothing back
	err = env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte("hello"))
		if err != nil {
			return err
		}
		fmt.Printf("key 'hello' retreived value: '%v'\n", string(v))
		return nil
	})
	_ = err

	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		panicOn(err)
		defer cur.Close()

		var cur2 *lmdb.Cursor
		var err2 error
		var k, k2, v, v2 []byte
		i := 0
		for {
			if i == 0 {
				// lmdb.SetRange : The first key no less than the specified key.
				k, v, err = cur.Get([]byte("200"), nil, lmdb.SetRange)

				// cur2 should start at 'a'
				cur2, err2 = txn.OpenCursor(dbi)
				panicOn(err2)
				defer cur2.Close()
				k2, v2, err2 = cur2.Get(nil, nil, lmdb.Next)
				_ = err2
			} else {
				k, v, err = cur.Get(nil, nil, lmdb.Next)
				k2, v2, err2 = cur2.Get(nil, nil, lmdb.Next)
				_ = err2
			}
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			fmt.Printf("i=%v, %s %s\n", i, k, v)
			fmt.Printf("i=%v, k2:%s v2:%s\n", i, k2, v2)
			i++
		}
		// return nil // unreachable
	})
	_ = err

	// panicOn(txn.Put(dbi, []byte("099"), []byte("A"), 0))
	// panicOn(txn.Put(dbi, []byte("101"), []byte("B"), 0))
	// panicOn(txn.Put(dbi, []byte("199"), []byte("C"), 0))
	// panicOn(txn.Put(dbi, []byte("200"), []byte("D"), 0))
	// panicOn(txn.Put(dbi, []byte("300"), []byte("E"), 0))
	// panicOn(txn.Put(dbi, []byte("399"), []byte("F"), 0))
	//
	// find max in [300,400) and get 399
	// find max in [000,100) and get 099
	// find max in [100,200) and get 199
	// find max in [400,500) and get nothing back
	// find max in [201,300) and get nothing back

	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		panicOn(err)
		defer cur.Close()

		var k, v []byte

		// find max in [300,400) and get 399

		// lmdb.SetRange : The first key no less than the specified key.
		k, v, err = cur.Get([]byte("400"), nil, lmdb.SetRange)
		if lmdb.IsNotFound(err) {
			fmt.Printf("400 not found, as expected\n") // happens on starting empty db
		} else {
			fmt.Printf("Get 400 => %v: %v\n", string(k), string(v))
		}

		k, v, err = cur.Get(nil, nil, lmdb.Prev)
		if lmdb.IsNotFound(err) {
			fmt.Printf("Get 400 then Get Prev => not found\n")
		} else {
			fmt.Printf("Get 400 then Get Prev => %v: %v\n", string(k), string(v)) // 399: F, so wraps backwards from beginning.
		}
		panicOn(err)

		// now try for 199 in [100,200)

		k, v, err = cur.Get([]byte("200"), nil, lmdb.SetRange)
		if lmdb.IsNotFound(err) {
			panic("200 not found, not expected")
		} else {
			fmt.Printf("Get 200 => %v: %v\n", string(k), string(v)) // Get 200 => 200: D
		}

		k, v, err = cur.Get(nil, nil, lmdb.Prev)
		if lmdb.IsNotFound(err) {
			fmt.Printf("Get 200 then Get Prev => not found\n")
		} else {
			fmt.Printf("Get 200 then Get Prev => %v: %v\n", string(k), string(v)) // Get 200 then Get Prev => 199: C
		}
		panicOn(err)

		k, v, err = cur.Get([]byte("500"), nil, lmdb.SetRange)
		if lmdb.IsNotFound(err) {
			fmt.Printf("500 not found, as expected\n") // 500 not found, as expected
		} else {
			panic(fmt.Sprintf("Get 500 => %v: %v\n", string(k), string(v)))
		}

		k, v, err = cur.Get(nil, nil, lmdb.Prev)
		if lmdb.IsNotFound(err) {
			fmt.Printf("Get 500 then Get Prev => not found\n")
		} else {
			fmt.Printf("Get 500 then Get Prev => %v: %v\n", string(k), string(v)) // Get 500 then Get Prev => 399: F
		}
		panicOn(err)

		k, v, err = cur.Get([]byte("100"), nil, lmdb.SetRange)
		if lmdb.IsNotFound(err) {
			panic("100 not found, not expected")
		} else {
			fmt.Printf("Get 100 => %v: %v\n", string(k), string(v)) // Get 100 => 101: B
		}

		k, v, err = cur.Get(nil, nil, lmdb.Prev)
		if lmdb.IsNotFound(err) {
			fmt.Printf("Get 100 then Get Prev => not found\n")
		} else {
			fmt.Printf("Get 100 then Get Prev => %v: %v\n", string(k), string(v)) // Get 100 then Get Prev => 099: A
		}
		panicOn(err)

		// find max in [201,300) and get nothing back

		k, v, err = cur.Get([]byte("300"), nil, lmdb.SetRange)
		if lmdb.IsNotFound(err) {
			panic("300 not found, not expected")
		} else {
			fmt.Printf("Get 300 => %v: %v\n", string(k), string(v)) // Get 300 => 300: E
		}

		k, v, err = cur.Get(nil, nil, lmdb.Prev)
		if lmdb.IsNotFound(err) {
			fmt.Printf("Get 300 then Get Prev => not found\n")
		} else {
			fmt.Printf("Get 300 then Get Prev => %v: %v\n", string(k), string(v)) // Get 300 then Get Prev => 200: D
		}
		cmp := bytes.Compare(k, []byte("201"))
		if cmp >= 0 {
			fmt.Printf("key k = '%v' was >= 201", string(k))
		} else {
			fmt.Printf("key k = '%v' was < 201", string(k)) // key k = '200' was < 201
		}
		panicOn(err)

		return nil
	})
	panicOn(err)

	vv("done")
}
