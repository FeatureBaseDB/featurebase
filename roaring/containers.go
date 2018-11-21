// Copyright (C) 2017-2018 Pilosa Corp. All rights reserved.
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

package roaring

type sliceContainers struct {
	keys          []uint64
	containers    []*Container
	lastKey       uint64
	lastContainer *Container
}

func newSliceContainers() *sliceContainers {
	return &sliceContainers{}
}

func (sc *sliceContainers) Get(key uint64) *Container {
	i := search64(sc.keys, key)
	if i < 0 {
		return nil
	}
	return sc.containers[i]
}

func (sc *sliceContainers) Put(key uint64, c *Container) {
	i := search64(sc.keys, key)

	// If index is negative then there's not an exact match
	// and a container needs to be added.
	if i < 0 {
		sc.insertAt(key, c, -i-1)
	} else {
		sc.containers[i] = c
	}

}

func (sc *sliceContainers) PutContainerValues(key uint64, containerType byte, n int, mapped bool) {
	i := search64(sc.keys, key)
	if i < 0 {
		c := NewContainer()
		c.containerType = containerType
		c.n = int32(n)
		c.mapped = mapped
		sc.insertAt(key, c, -i-1)
	} else {
		c := sc.containers[i]
		c.containerType = containerType
		c.n = int32(n)
		c.mapped = mapped
	}

}

func (sc *sliceContainers) Remove(key uint64) {
	statsHit("sliceContainers/Remove")
	i := search64(sc.keys, key)
	if i < 0 {
		return
	}
	sc.keys = append(sc.keys[:i], sc.keys[i+1:]...)
	sc.containers = append(sc.containers[:i], sc.containers[i+1:]...)

}
func (sc *sliceContainers) insertAt(key uint64, c *Container, i int) {
	statsHit("sliceContainers/insertAt")
	sc.keys = append(sc.keys, 0)
	copy(sc.keys[i+1:], sc.keys[i:])
	sc.keys[i] = key

	sc.containers = append(sc.containers, nil)
	copy(sc.containers[i+1:], sc.containers[i:])
	sc.containers[i] = c
}

func (sc *sliceContainers) GetOrCreate(key uint64) *Container {
	// Check the last* cache for same container.
	if key == sc.lastKey && sc.lastContainer != nil {
		return sc.lastContainer
	}

	sc.lastKey = key
	i := search64(sc.keys, key)
	if i < 0 {
		c := NewContainer()
		sc.insertAt(key, c, -i-1)
		sc.lastContainer = c
		return c
	}

	sc.lastContainer = sc.containers[i]
	return sc.lastContainer
}

func (sc *sliceContainers) Clone() Containers {
	other := newSliceContainers()
	other.keys = make([]uint64, len(sc.keys))
	other.containers = make([]*Container, len(sc.containers))
	copy(other.keys, sc.keys)
	for i, c := range sc.containers {
		other.containers[i] = c.Clone()
	}
	return other
}

func (sc *sliceContainers) Last() (key uint64, c *Container) {
	if len(sc.keys) == 0 {
		return 0, nil
	}
	return sc.keys[len(sc.keys)-1], sc.containers[len(sc.keys)-1]
}

func (sc *sliceContainers) Size() int {
	return len(sc.keys)

}

func (sc *sliceContainers) Count() uint64 {
	n := uint64(0)
	for i := range sc.containers {
		n += uint64(sc.containers[i].n)
	}
	return n
}

func (sc *sliceContainers) Reset() {
	sc.keys = sc.keys[:0]
	sc.containers = sc.containers[:0]
	sc.lastContainer = nil
	sc.lastKey = 0
}

func (sc *sliceContainers) seek(key uint64) (int, bool) {
	i := search64(sc.keys, key)
	found := true
	if i < 0 {
		found = false
		i = -i - 1
	}
	return i, found
}

func (sc *sliceContainers) Iterator(key uint64) (citer ContainerIterator, found bool) {
	i, found := sc.seek(key)
	return &sliceIterator{e: sc, i: i}, found
}

type sliceIterator struct {
	e     *sliceContainers
	i     int
	key   uint64
	value *Container
}

func (si *sliceIterator) Next() bool {
	if si.e == nil || si.i > len(si.e.keys)-1 {
		return false
	}
	si.key = si.e.keys[si.i]
	si.value = si.e.containers[si.i]
	si.i++

	return true
}

func (si *sliceIterator) Value() (uint64, *Container) {
	return si.key, si.value
}
