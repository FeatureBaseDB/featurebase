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
	"encoding/binary"
	"fmt"
	"sync"

	cryptorand "crypto/rand"
	"github.com/zeebo/blake3"
	"golang.org/x/mod/sumdb/dirhash"
)

// Blake3Hasher is a thread/goroutine safe way to
// obtain a blake3 cryptographic hash of input []byte.
// Reference https://github.com/BLAKE3-team/BLAKE3
// suggests it is 6x faster than BLAKE2B.
// The Go github.com/zeebo/blake3 version is
// AVX2 and SSE4.1 accelerated.
type Blake3Hasher struct {
	hasher   *blake3.Hasher
	hasherMu sync.Mutex
}

// NewBlake3Hasher returns a new Blake3Hasher.
func NewBlake3Hasher() *Blake3Hasher {
	return &Blake3Hasher{
		hasher: blake3.New(),
	}
}

// CryptoHash writes the blake3 cryptographic hash of
// input into buffer and returns it.
// Like the standard libary's hash.Hash interface's Sum() method,
// the buffer is re-used and overwritten
// to avoid allocation. The caller determines the byte length of
// the outputCryptohash by the size of the supplied buffer
// slice, and this will be exactly equal to the supplies bytes.
// In this way, shorter or longer hashes can be provided as
// needed.
func (w *Blake3Hasher) CryptoHash(input []byte, buffer []byte) (outputCryptohash []byte) {
	w.hasherMu.Lock()
	w.hasher.Reset()

	// "Write implements part of the hash.Hash interface. It never returns an error."
	//  -- https://godoc.org/github.com/zeebo/blake3#Hasher.Write
	_, _ = w.hasher.Write(input)

	// Digest.Read reads data from the hasher into buffer.
	// "It always fills the entire buffer and never errors."
	//   -- https://godoc.org/github.com/zeebo/blake3#Digest
	_, _ = w.hasher.Digest().Read(buffer)

	// no chance of panic, so avoid any defer cost.
	w.hasherMu.Unlock()

	return buffer
}

// blake3sum16 might be slower because we allocate a new hasher every time, but
// it is more conenient for writing debug code. It returns
// a 16 byte hash as a hexidecimal string.
func Blake3sum16(input []byte) string {
	hasher := blake3.New()

	_, _ = hasher.Write(input)
	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])

	return fmt.Sprintf("%x", buf)
}

// cryptoRandInt64 uses crypto/rand to get an random int64
func cryptoRandInt64() int64 {
	c := 8
	b := make([]byte, c)
	_, err := cryptorand.Read(b)
	if err != nil {
		panic(err)
	}
	r := int64(binary.LittleEndian.Uint64(b))
	return r
}

func HashOfDir(path string) string {
	prefix := ""
	h, err := dirhash.HashDir(path, prefix, dirhash.Hash1)
	panicOn(err)
	return h
}
