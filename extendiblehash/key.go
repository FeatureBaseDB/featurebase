package extendiblehash

import (
	"github.com/zeebo/xxh3"
)

type Hashable interface {
	Hash() uint64
}

// use the same seed all the time - this is not for crypto
var protoSeed uint64 = 20041973

var hasher = xxh3.NewSeed(protoSeed)

type Key []byte

// BEWARE - not concurrent!!
func (k Key) Hash() uint64 {
	hasher.Reset()
	hasher.Write(k)
	hash := hasher.Sum64()
	return hash
}
