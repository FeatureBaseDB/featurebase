package roaring

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// randomBitmap generates a random Bitmap with Ncontainers.
// The type and cardinality of each container is random; keys are sequential.
func randomBitmap(Ncontainers int) *Bitmap {
	b := &Bitmap{
		keys:       make([]uint64, Ncontainers),
		containers: make([]*container, Ncontainers),
	}

	for n := 0; n < Ncontainers; n++ {
		b.keys[n] = uint64(n)
		// The intent here is to generate containers with uniformly distributed container
		// type, but after b.Optimize(), that won't be the case. As long as we get some
		// amount of each type, this still serves its purpose.
		// The Right Way would be to generate bitsets that get optimized into the correct
		// type, i.e. calling a function randomBitset(N, Nruns) with appropriate arguments.
		switch rand.Intn(3) {
		case 0:
			// Could be array or RLE.
			b.containers[n] = randomArray(rand.Intn(ArrayMaxSize-1) + 1)
		case 1:
			// Guaranteed bitmap.
			b.containers[n] = randomBitset(rand.Intn(65536-ArrayMaxSize) + ArrayMaxSize)
		case 2:
			// Probably RLE.
			b.containers[n] = randomRunset(rand.Intn(RunMaxSize-1) + 1)
		}

	}
	b.Optimize()
	return b
}

// randomArray generates an array container with N elements.
func randomArray(N int) *container {
	c := &container{n: N}
	vals := rand.Perm(65536)[0:N]
	sort.Ints(vals)
	c.array = make([]uint32, N)
	for n := 0; n < N; n++ {
		c.array[n] = uint32(vals[n])
	}
	return c
}

// randomBitset generates a bitmap container with N elements.
func randomBitset(N int) *container {
	c := &container{n: N}
	vals := rand.Perm(65536)
	c.bitmap = make([]uint64, bitmapN)
	for n := 0; n < N; n++ {
		c.bitmap[vals[n]/64] |= (1 << uint64(vals[n]%64))
	}
	return c
}

// randomRunset generates an RLE container with N runs.
func randomRunset(N int) *container {
	c := &container{}
	vals := rand.Perm(65536)[0 : 2*N]
	sort.Ints(vals)
	c.runs = make([]interval32, N)
	c.n = 0
	for n := 0; n < N; n++ {
		c.runs[n] = interval32{
			start: uint32(vals[2*n]),
			last:  uint32(vals[2*n+1]),
		}
		c.n += vals[2*n+1] - vals[2*n] + 1
	}
	return c
}

func (b *Bitmap) DebugInfo() {
	info := b.Info()
	for n, c := range b.containers {
		fmt.Printf("%3v %5v %6v\n", info.Containers[n].Key, c.n, info.Containers[n].Type)
	}
}

func TestWriteRead(t *testing.T) {
	rand.Seed(5)
	Ncontainers := 100
	iterations := 10
	for i := 0; i < iterations; i++ {
		b := randomBitmap(Ncontainers)
		// b.DebugInfo()
		b2 := &Bitmap{}

		var buf bytes.Buffer
		_, err := b.WriteTo(&buf)
		if err != nil {
			t.Fatalf("error writing: %v", err)
		}

		err = b2.UnmarshalBinary(buf.Bytes())
		if err != nil {
			t.Fatalf("error unmarshaling: %v", err)
		}

		if !b.Equal(*b2) {
			t.Fatalf("bitmap %d mismatch, exp \n%+v, got \n%+v", i, b, b2)
		}
	}
}
