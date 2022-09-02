package gen

import (
	"math/rand"
)

type Gen struct {
	seed int64

	// cache of zipfs for various imax. All have  s==1.1 and v==10
	zipfs map[uint64]*rand.Zipf

	R *rand.Rand
}

type Opt func(g *Gen)

// OptGenSeed is a functional option for providing a seed
// to Gen.
func OptGenSeed(s int64) Opt {
	return func(g *Gen) {
		g.seed = s
	}
}

// New creates a new *Gen
func New(opts ...Opt) *Gen {
	g := &Gen{
		seed:  17,
		zipfs: make(map[uint64]*rand.Zipf),
	}
	for _, opt := range opts {
		opt(g)
	}
	g.R = rand.New(rand.NewSource(g.seed))
	return g
}

var alphaUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
var alphaLower = "abcdefghijklmnopqrstuvwxyz"

func (g *Gen) AlphaUpper(bs []byte) {
	for i := range bs {
		bs[i] = alphaUpper[g.R.Intn(len(alphaUpper))]
	}
}

func (g *Gen) AlphaLower(bs []byte) {
	for i := range bs {
		bs[i] = alphaLower[g.R.Intn(len(alphaLower))]
	}
}

// StringSliceFromList repopulates input with a random number of items
// from list. If input does not have enough capacity it may return a
// new list. There may be repeat items, there may be 0 items, there
// will never be more items than the length of list.
func (g *Gen) StringSliceFromList(input []string, list []string) []string {
	input = input[:0]
	num := g.R.Intn(len(list) + 1)
	for i := 0; i < num; i++ {
		input = append(input, list[g.R.Intn(len(list))])
	}

	return input
}

// StringSliceFromListWeighted picks between 0 and upto items from list randomly, but zipfian weighted toward items earlier in list. It will try to re-use input, but may allocate a new list if needed.
func (g *Gen) StringSliceFromListWeighted(input []string, list []string, s, v float64) []string {
	input = input[:0]
	z := g.getZipfParams(uint64(len(list)-1), s, v)
	num := z.Uint64()
	for i := uint64(0); i < num; i++ {
		input = append(input, list[z.Uint64()])
	}
	return input
}

// StringFromListWeighted returns a random string from the list
// weighted toward the items earlier in the list.
func (g *Gen) StringFromListWeighted(list []string) string {
	z := g.getZipf(uint64(len(list) - 1))
	return list[z.Uint64()]
}

// StringFromList returns a random string from the list
// each with uniform probability
func (g *Gen) StringFromList(list []string) string {
	return list[g.R.Intn(len(list))]
}

// Set generates a random set of integers between min and max (inclusive).
// The mean cardinality of the set is specified by typicalCardinality.
func (g *Gen) Set(min uint64, max uint64, typicalCardinality uint64) map[uint64]struct{} {
	// The expected value of a negative binomial distribution is E(X) = r/p.
	// In this case we want E(x) = typicalCardinality.
	// In this context, a success means that we stop producing elements, so r = 1.
	// Therefore, p = 1/typicalCardinality.
	p := 1 / float64(typicalCardinality)

	// Create a random target cardinality.
	var n uint64
	for n < max-min && g.R.Float64() > p {
		n++
	}

	set := map[uint64]struct{}{}

	if n > (max-min)/2 {
		// The target cardinality is a substantial portion of the entire range.
		// Generating random values and checking would result in many collisions and raise the complexity.
		// Generate a list of values, shuffle it, and select the first n entries.
		list := make([]uint64, max-min)
		for i := range list {
			list[i] = uint64(i) + min
		}
		g.R.Shuffle(len(list), func(i, j int) {
			list[j], list[i] = list[i], list[j]
		})
		list = list[:n]
		for _, v := range list {
			set[v] = struct{}{}
		}
	} else {
		// The target cardinality is a small portion of the range.
		// Randomly selecting values should be fairly fast since collisions should be somewhat infrequent.
		for uint64(len(set)) < n {
			// Generate another value for the set.
			set[min+uint64(g.R.Int63n(int64((max-min)+1)))] = struct{}{}
		}
	}

	return set
}

func (g *Gen) getZipf(imax uint64) *rand.Zipf {
	if nz, ok := g.zipfs[imax]; !ok {
		nz = rand.NewZipf(g.R, 1.1, 10, imax)
		g.zipfs[imax] = nz
		return nz
	} else {
		return nz
	}
}

func (g *Gen) getZipfParams(imax uint64, s, v float64) *rand.Zipf {
	if nz, ok := g.zipfs[imax]; !ok {
		nz = rand.NewZipf(g.R, s, v, imax)
		g.zipfs[imax] = nz
		return nz
	} else {
		return nz
	}
}
