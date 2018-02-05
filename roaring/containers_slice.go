package roaring

func NewSliceContainers() *SliceContainers {
	return &SliceContainers{}
}

type SliceContainers struct {
	keys          []uint64
	containers    []*container
	lastKey       uint64
	lastContainer *container
}

func (sc *SliceContainers) Get(key uint64) *container {
	i := search64(sc.keys, key)
	if i < 0 {
		return nil
	}
	return sc.containers[i]
}

func (sc *SliceContainers) Put(key uint64, c *container) {
	i := search64(sc.keys, key)

	// If index is negative then there's not an exact match
	// and a container needs to be added.
	if i < 0 {
		sc.insertAt(key, c, -i-1)
	} else {
		sc.containers[i] = c
	}

}

func (sc *SliceContainers) PutContainerValues(key uint64, containerType byte, n int, mapped bool) {
	i := search64(sc.keys, key)
	if i < 0 {
		c := newContainer()
		c.containerType = containerType
		c.n = n
		c.mapped = mapped
		sc.insertAt(key, c, -i-1)
	} else {
		c := sc.containers[i]
		c.containerType = containerType
		c.n = n
		c.mapped = mapped
	}

}

func (sc *SliceContainers) Remove(key uint64) {
	i := search64(sc.keys, key)
	if i < 0 {
		return
	}
	sc.keys = append(sc.keys[:i], sc.keys[i+1:]...)
	sc.containers = append(sc.containers[:i], sc.containers[i+1:]...)

}
func (sc *SliceContainers) insertAt(key uint64, c *container, i int) {
	sc.keys = append(sc.keys, 0)
	copy(sc.keys[i+1:], sc.keys[i:])
	sc.keys[i] = key

	sc.containers = append(sc.containers, nil)
	copy(sc.containers[i+1:], sc.containers[i:])
	sc.containers[i] = c
}

func (sc *SliceContainers) GetOrCreate(key uint64) *container {
	// Check the last* cache for same container.
	if key == sc.lastKey && sc.lastContainer != nil {
		return sc.lastContainer
	}

	sc.lastKey = key
	i := search64(sc.keys, key)
	if i < 0 {
		c := newContainer()
		sc.insertAt(key, c, -i-1)
		sc.lastContainer = c
		return c
	}

	sc.lastContainer = sc.containers[i]
	return sc.lastContainer
}

func (sc *SliceContainers) Clone() Containers {
	other := NewSliceContainers()
	other.keys = make([]uint64, len(sc.keys))
	other.containers = make([]*container, len(sc.containers))
	copy(other.keys, sc.keys)
	for i, c := range sc.containers {
		other.containers[i] = c.clone()
	}
	return other
}

func (sc *SliceContainers) Last() (key uint64, c *container) {
	if len(sc.keys) == 0 {
		return 0, nil
	}
	return sc.keys[len(sc.keys)-1], sc.containers[len(sc.keys)-1]
}

func (sc *SliceContainers) Size() int {
	return len(sc.keys)

}

func (sc *SliceContainers) seek(key uint64) (int, bool) {
	i := search64(sc.keys, key)
	found := true
	if i < 0 {
		found = false
		i = -i - 1
	}
	return i, found
}

func (sc *SliceContainers) Iterator(key uint64) (citer Contiterator, found bool) {
	i, found := sc.seek(key)
	return &SliceIterator{e: sc, i: i}, found
}

type SliceIterator struct {
	e     *SliceContainers
	i     int
	key   uint64
	value *container
}

func (si *SliceIterator) Next() bool {
	if si.e == nil || si.i > len(si.e.keys)-1 {
		return false
	}
	si.key = si.e.keys[si.i]
	si.value = si.e.containers[si.i]
	si.i++
	return true
}

func (si *SliceIterator) Value() (uint64, *container) {
	return si.key, si.value
}
