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
	"github.com/pilosa/pilosa/v2/hash"
	"github.com/zeebo/blake3"
)

// pilosa-chk : read boltdb files and print checksums and counts on the keys. With
// -v and -ops and -bits you can display every last bit if you want.
//
// pilosa-chk is deliberately NOT a part of pilosa so that it can run without
// forcing a customer to upgrade or downgrade their installed version.

func main() {

	var dir string
	var showOpsLog bool
	var showBits bool
	var showFrags bool
	var dirChecksum bool
	home := os.Getenv("HOME")
	flag.StringVar(&dir, "dir", fmt.Sprintf("%v/.pilosa", home), "pilosa data dir to read")
	flag.BoolVar(&showFrags, "v", false, "show the checksum hash for each fragment in each index. Warning: long output")
	flag.BoolVar(&showOpsLog, "ops", false, "show the ops log for each fragment. Warning: very long output. Implies -v")
	flag.BoolVar(&showBits, "bits", false, "show the hot bits for each fragment. Warning: very, very long output. Implies -v")
	flag.BoolVar(&dirChecksum, "dirsum", false, "compute a directory hash")
	flag.Parse()

	if showBits {
		showFrags = true
	}
	if showOpsLog {
		showFrags = true
	}
	fmt.Printf("opening dir '%v'... this may take a few seconds...\n", dir)

	if dirChecksum {
		fmt.Printf("path '%v' has dirhash %v\n", dir, hash.HashOfDir(dir))
		return
	}

	fmt.Printf("    the blake-3 hash includes the value of each mapping and the field or partitionID.\n")

	holder := pilosa.NewHolder(dir, nil)
	holder.OpenTranslateStore = boltdb.OpenTranslateStore

	err := holder.Open()

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\ncalculating hashes of row and column key translation maps on data from dir '%v'...\n", dir)
	var indexes []*pilosa.Index

	final := pilosa.NewAllTranslatorSummary()
	const verbose = true
	for _, idx := range holder.Indexes() {
		asum, err := idx.ComputeTranslatorSummary(verbose)
		if err != nil {
			log.Fatal(err)
		}
		final.Append(asum)
		indexes = append(indexes, idx)
	}
	final.Sort()

	hasher := blake3.New()
	fmt.Printf("\nsummary of col/row translations%v:\n", dir)
	for _, sum := range final.Sums {
		//fmt.Printf("index: %v  partitionID: %v blake3-%x keyCount: %v idCount: %v\n", sum.Index, sum.PartitionID, sum.Checksum, sum.KeyCount, sum.IDCount)
		_, _ = hasher.Write([]byte(sum.Checksum))
	}

	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])

	fmt.Printf("all-checksum = blake3-%x\n", buf)

	if showFrags {
		for _, idx := range indexes {
			fmt.Printf("==============================\n")
			fmt.Printf("index: %v\n", idx.Name())
			fmt.Printf("==============================\n")
			idx.WriteFragmentChecksums(os.Stdout, showBits, showOpsLog)
		}
	}
}
