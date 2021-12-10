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
	sc.lastKey = key
	sc.lastContainer = c
}

func (sc *sliceContainers) Remove(key uint64) {
	statsHit("sliceContainers/Remove")
	i := search64(sc.keys, key)
	if i < 0 {
		return
	}
	if key == sc.lastKey {
		sc.invalidateCache()
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
		if c == nil {
			other.containers[i] = nil
			continue
		}
		other.containers[i] = c.Clone()
	}
	return other
}

func (sc *sliceContainers) Freeze() Containers {
	other := newSliceContainers()
	other.keys = make([]uint64, len(sc.keys))
	other.containers = make([]*Container, len(sc.containers))
	copy(other.keys, sc.keys)
	for i, c := range sc.containers {
		other.containers[i] = c.Freeze()
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
		n += uint64(sc.containers[i].N())
	}
	return n
}

func (sc *sliceContainers) Reset() {
	sc.keys = sc.keys[:0]
	sc.containers = sc.containers[:0]
	sc.invalidateCache()
}

func (sc *sliceContainers) ResetN(n int) {
	if cap(sc.keys) < n {
		sc.keys = make([]uint64, 0, n)
		sc.containers = make([]*Container, 0, n)
	} else {
		sc.keys = sc.keys[:0]
		sc.containers = sc.containers[:0]
	}
	sc.invalidateCache()
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
	return &sliceIterator{e: sc, i: i, index: i}, found
}

// Repair tries to repair all containers,
// results in nil and empty containers getting dropped from the slice.
// For instance, that has to happen for writing the roaring format,
// which can't represent an empty container (c.N() == 0).
func (sc *sliceContainers) Repair() {
	n := 0
	for i, c := range sc.containers {
		if c == nil {
			continue
		}
		c.Repair()
		sc.containers[n] = c
		sc.keys[n] = sc.keys[i]
		n++
	}
	sc.containers = sc.containers[:n]
	sc.keys = sc.keys[:n]

	sc.invalidateCache()
}

// Update calls fn (existing-container, existed), and expects
// (new-container, write). If write is true, the container is used to
// replace the given container.
func (sc *sliceContainers) Update(key uint64, fn func(*Container, bool) (*Container, bool)) {
	i, found := sc.seek(key)
	var nc *Container
	var write bool
	if found {
		nc, write = fn(sc.containers[i], true)
		if write {
			sc.containers[i] = nc
		}
	} else {
		nc, write = fn(nil, false)
		// don't expand the slice just to add a nil container, we
		// could return that anyway
		if write && nc != nil {
			sc.insertAt(key, nc, i)
		}
	}
	sc.invalidateCache()
}

// UpdateEvery calls fn (existing-container, existed), and expects
// (new-container, write). If write is true, the container is used to
// replace the given container.
func (sc *sliceContainers) UpdateEvery(fn func(uint64, *Container, bool) (*Container, bool)) {
	for i, c := range sc.containers {
		nc, write := fn(sc.keys[i], c, true)
		if write {
			sc.containers[i] = nc
		}
	}

	sc.invalidateCache()
}

func (sc *sliceContainers) invalidateCache() {
	sc.lastKey = ^uint64(0)
	sc.lastContainer = nil
}

type sliceIterator struct {
	e     *sliceContainers
	i     int        // next e's index to get key, value
	index int        // current e's index of key, value
	key   uint64     // current key
	value *Container // current value
}

func (si *sliceIterator) Close() {}

func (si *sliceIterator) Next() bool {
	if si.e == nil {
		return false
	}

	// discard nil containers from iteration. we don't always
	// actually remove them because copying is expensive.
	for si.i < len(si.e.keys) {
		si.key = si.e.keys[si.i]
		si.value = si.e.containers[si.i]
		// keep the current index of key, value
		si.index = si.i
		si.i++
		if si.value != nil {
			return true
		}
	}
	return false
}

func (si *sliceIterator) Value() (uint64, *Container) {
	return si.key, si.value
}
