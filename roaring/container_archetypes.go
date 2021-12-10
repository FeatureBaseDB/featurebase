// Copyright 2021 Molecula Corp. All rights reserved.
package roaring

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/molecula/apophenia"
)

// ContainerArchetypeNames is the list of supported container archetypes
// used in some testing. This is exported for use in a benchmark-analysis
// tool.
var ContainerArchetypeNames = []string{
	"Empty",
	"Ary1",
	"Ary16",
	"Ary256",
	"Ary512",
	"Ary1024",
	"Ary4096",
	"RunFull",
	"RunSplit",
	"Run16",
	"Run16Small",
	"Run256",
	"Run256Small",
	"Run1024",
	"BM512",
	"BM1024",
	"BM4096",
	"BM4097",
	"BM32768",
	"BM65000",
}

var containerArchetypes [][]*Container
var containerArchetypesErr error
var initContainerArchetypes sync.Once

func makeArchetypalContainer(rng *rand.Rand, name string) (*Container, error) {
	var c *Container
	switch {
	case name == "Empty":
		c = NewContainerArray(nil)
	case strings.HasPrefix(name, "Ary"):
		size, err := strconv.Atoi(name[3:])
		if err != nil {
			return nil, fmt.Errorf("can't parse array size: %v", err)
		}
		array := make([]uint16, size)
		seq := apophenia.NewSequence(rng.Int63())
		perm, err := apophenia.NewPermutation(65536, 0, seq)
		if err != nil {
			return nil, err
		}
		for i := 0; i < size; i++ {
			array[i] = uint16(perm.Next())
		}
		sort.Slice(array, func(a, b int) bool { return array[a] < array[b] })
		c = NewContainerArray(array)
	case name == "RunFull":
		c = NewContainerRun([]Interval16{{Start: 0, Last: 65535}})
	case name == "RunSplit":
		runs := []Interval16{
			{Start: 0, Last: 32700 + uint16(rng.Intn(30))},
			{Start: 32768 + uint16(rng.Intn(30)), Last: 65535},
		}
		c = NewContainerRun(runs)
	case strings.HasPrefix(name, "Run"):
		countEnd := len(name)
		small := strings.HasSuffix(name, "Small")
		if small {
			countEnd -= 5
		}
		count, err := strconv.Atoi(name[3:countEnd])
		if err != nil {
			return nil, fmt.Errorf("can't parse run count in '%s': %v", name, err)
		}
		runs := make([]Interval16, count)
		// For size intervals, we want to divvy up the total
		// space around count+1 points, and populate the space between
		// those points with a run smaller than that.
		stride := int32(65535 / (count + 1))
		upper := stride - 10
		lower := stride / 10
		if small {
			lower = 3
			upper = lower + (stride / 20)
		}
		variance := upper - lower
		next := int32(0)
		prev := int32(0)
		for i := 0; i < count; i++ {
			next += stride
			middle := (prev + next) / 2
			runSize := rng.Int31n(variance) + lower
			offset := rng.Int31n(variance)
			runs[i].Start = uint16(middle + offset - (runSize / 2))
			runs[i].Last = runs[i].Start + uint16(runSize)
			if runs[i].Last < runs[i].Start {
				return nil, fmt.Errorf("fatal, run %d starts at %d, tries to end at %d", i, runs[i].Start, runs[i].Last)
			}
			prev = next
			if i > 0 {
				if runs[i].Start <= runs[i-1].Last {
					if runs[i-1].Last > 65533 {
						return nil, fmt.Errorf("fatal, run %d starts at %d, previous run ended at %d",
							i, runs[i].Start, runs[i-1].Last)
					} else {
						runs[i].Start = runs[i-1].Last + 2
						if runs[i].Last < runs[i].Start {
							runs[i].Last = runs[i].Start
						}
					}
				}
			}
		}
		c = NewContainerRun(runs)
	case strings.HasPrefix(name, "BM"):
		size, err := strconv.Atoi(name[2:])
		if err != nil {
			return nil, fmt.Errorf("can't parse bitmap size: %v", err)
		}
		bitmap := make([]uint64, bitmapN)
		n := int32(0)
		flip := false
		// Picking random bits sometimes overlaps, so we want to
		// keep trying until we get the requested number. But that's
		// really slow for N close to the maximum, so if we want more
		// than half the bits set, we'll do it backwards and then
		// invert the bits.
		bits := int32(size)
		if size > 32768 {
			flip = true
			bits = 65536 - bits
		}
		for n < bits {
			pos := (rng.Uint64() & 65535)
			bit := uint64(1 << (pos & 63))
			if bitmap[pos/64]&bit == 0 {
				n++
				bitmap[pos/64] |= bit
			}
		}
		if flip {
			for i := 0; i < bitmapN; i++ {
				bitmap[i] = ^bitmap[i]
			}
			n = 65536 - n
		}
		c = NewContainerBitmap(int(n), bitmap)
		count := c.count()
		if count != n {
			return nil, fmt.Errorf("bitmap should have %d bits, has %d", n, count)
		}
	}
	return c, nil
}

// InitContainerArchetypes ensures that createContainerArchetypes has been
// called, and returns the results of that one call.
func InitContainerArchetypes() ([][]*Container, error) {
	initContainerArchetypes.Do(func() {
		containerArchetypes, containerArchetypesErr = createContainerArchetypes(8)
	})
	return containerArchetypes, containerArchetypesErr
}

// createContainerArchetypes creates a slice of *roaring.Container corresponding
// to each container archetype, or reports an error.
func createContainerArchetypes(count int) (cats [][]*Container, err error) {
	cats = make([][]*Container, len(ContainerArchetypeNames))
	// seed is arbitrary, but picking a seed means we don't get different
	// behavior for each run
	rng := rand.New(rand.NewSource(23))
	for i, name := range ContainerArchetypeNames {
		cats[i] = make([]*Container, count)
		for j := 0; j < count; j++ {
			c, err := makeArchetypalContainer(rng, name)
			if err != nil {
				return nil, err
			}
			cats[i][j] = c
		}
	}
	return cats, nil
}
