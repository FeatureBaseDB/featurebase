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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/zeebo/blake3"
)

// translatorchk : read boltdb files and print checksums and counts on the keys.

func main() {

	var dir string
	home := os.Getenv("HOME")
	flag.StringVar(&dir, "dir", fmt.Sprintf("%v/.pilosa", home), "pilosa data dir to read")
	flag.Parse()

	fmt.Printf("opening dir '%v'... this may take a few seconds...\n", dir)

	holder := pilosa.NewHolder(256)
	holder.Path = dir
	holder.OpenTranslateStore = boltdb.OpenTranslateStore

	err := holder.Open()

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\ncalculating checksums on data from dir '%v'...\n", dir)

	final := pilosa.NewAllTranslatorSummary()
	const verbose = true
	for _, idx := range holder.Indexes() {
		asum, err := idx.ComputeTranslatorSummary(verbose)
		if err != nil {
			log.Fatal(err)
		}
		final.Merge(asum)
	}
	final.Sort()

	hasher := blake3.New()
	fmt.Printf("\nsummary of %v:\n", dir)
	for _, sum := range final.Sums {
		//fmt.Printf("index: %v  partitionID: %v blake3-%x keyCount: %v idCount: %v\n", sum.Index, sum.PartitionID, sum.Checksum, sum.KeyCount, sum.IDCount)
		_, _ = hasher.Write(sum.Checksum)
	}

	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])

	fmt.Printf("all-checksum = blake3-%x\n", buf)
}
