package roaring

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func randomBitmap(Ncontainers int) *Bitmap {
	b := &Bitmap{
		keys:       make([]uint64, Ncontainers),
		containers: make([]*container, Ncontainers),
	}

	for n := 0; n < Ncontainers; n++ {
		b.keys[n] = uint64(n)
		// We could generate only bitmaps here and then depend on Optimize()
		// to convert to the appropriate container types. However, certain
		// sets can be correctly represented as either array or runs.
		// Generating the desired type directly encourages a more uniform
		// distribution of container types. This is not perfect (TODO)
		switch rand.Intn(3) {
		case 0:
			b.containers[n] = randomArray(rand.Intn(ArrayMaxSize))
			// fmt.Printf("%d array len = %d\n", n, len(b.containers[n].array))
		case 1:
			b.containers[n] = randomBitset(rand.Intn(65536))
			// fmt.Printf("%d bitmap len = %d\n", n, len(b.containers[n].bitmap))
		case 2:
			b.containers[n] = randomRunset(rand.Intn(RunMaxSize))
			// fmt.Printf("%d runs len = %d\n", n, len(b.containers[n].runs))
		}

	}
	b.Optimize()
	return b
}

func randomArray(N int) *container {
	// generate array container with N elements
	c := &container{n: N}
	vals := rand.Perm(65536)[0:N]
	sort.Ints(vals)
	c.array = make([]uint32, N)
	for n := 0; n < N; n++ {
		c.array[n] = uint32(vals[n])
	}
	return c
}

func randomBitset(N int) *container {
	// generate bitmap container with N elements
	c := &container{n: N}
	vals := rand.Perm(65536)
	c.bitmap = make([]uint64, bitmapN)
	for n := 0; n < N; n++ {
		c.bitmap[vals[n]/64] |= (1 << uint64(vals[n]%64))
	}
	return c
}

func randomRunset(N int) *container {
	// generate run container with N elements
	c := randomArray(N)
	// c.arrayToRun()
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
		// fmt.Printf("\n%d\n", i)
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
