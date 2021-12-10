package pilosa

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// TestBSIAdd does a number of iterations. For each iteration, it
// generates a random number of ids, and two random values for each id
// to add together.
func TestBSIAdd(t *testing.T) {
	// TODO wouldn't it be cool if our test suite had a randomized
	// burn-in mode where you could run any test which supported it
	// with a random seed and way more iterations?
	rnd := rand.New(rand.NewSource(99))
	//numZipf := rand.NewZipf(rnd, 1.5, 2, ShardWidth-1)
	idZipf := rand.NewZipf(rnd, 1.8, 4, ShardWidth)
	var builderA, builderB bsiBuilder
	// a and b are generated slices of numbers to add together
	var a, b []uint64
	// idToIndex maps record ids to indexes in a and b
	idToIndex := make(map[int]int)
	// indexToID has the record id for each value in a and b
	indexToID := []uint64{}

	min := 999999999
	max := 0

	for iteration := 0; iteration < 1; iteration++ {
		t.Run(fmt.Sprintf("%d", iteration), func(t *testing.T) {
			// reset generated data
			a, b = a[:0], b[:0]
			indexToID = indexToID[:0]
			for k := range idToIndex {
				delete(idToIndex, k)
			}

			// z generates the values, they can be fairly large, but are usually small
			z := rand.NewZipf(rnd, 1.3, 7, 1<<44)
			id := -1
			for i := 0; true; i++ {
				// get the next id, skipping a random amount
				id = id + int(idZipf.Uint64()+1)
				if id >= ShardWidth {
					if i < min {
						min = i
					}
					if max < i {
						max = i
					}
					break
				}
				idToIndex[id] = int(i)
				indexToID = append(indexToID, uint64(id))

				// append a random value to each data slice
				a = append(a, z.Uint64())
				b = append(b, z.Uint64())
			}

			// build the BSIs based on the data slices and generated IDs
			for index, id := range indexToID {
				va, vb := a[index], b[index]
				builderA.Insert(uint64(id), va)
				builderB.Insert(uint64(id), vb)
			}
			dataA, dataB := builderA.Build(), builderB.Build()
			dataC := addBSI(dataA, dataB)

			// build results from added bsiData; results[i] should hold a[i]+b[i]
			results := make([]uint64, len(a))
			dataC.pivotDescending(NewRow().Union(dataC...), 0, nil, nil, func(count uint64, ids ...uint64) {
				for _, id := range ids {
					results[idToIndex[int(id)]] = count
				}
			})

			for i, res := range results {
				if res != a[i]+b[i] {
					t.Errorf("Mismatch at %d\na: %v\nb: %v\nr: %v", i, a, b, results)
				}
			}
		})
	}
}

type bsiAddCase struct {
	positions []uint64
	a         []uint64
	b         []uint64
}

func (b bsiAddCase) Len() int {
	return len(b.positions)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (b bsiAddCase) Less(i, j int) bool {
	return b.positions[i] < b.positions[j]
}

// Swap swaps the elements with indexes i and j.
func (b bsiAddCase) Swap(i, j int) {
	b.positions[i], b.positions[j] = b.positions[j], b.positions[i]
	b.a[i], b.a[j] = b.a[j], b.a[i]
	b.b[i], b.b[j] = b.b[j], b.b[i]
}

// TestBSIAddCases tests specific cases of bsiAdd (would generally be
// pulled from randomly generated ones from TestBSIAdd upon failure).
func TestBSIAddCases(t *testing.T) {
	tests := []bsiAddCase{
		{
			positions: []uint64{161311, 611110, 82544, 996022, 836077, 64964, 480737, 156534, 240525, 580896, 239236, 54607, 1019438, 894260, 17570, 884645, 936658, 682651, 987695, 390274},
			a:         []uint64{17, 1, 2846, 45437619, 23781, 36, 88, 168691, 13417, 1301, 10, 71, 0, 176, 1010, 21, 1, 509, 17, 4},
			b:         []uint64{24, 288, 12737, 14, 150, 21, 24, 354, 0, 19, 5, 150, 3940, 121, 25, 621, 7, 9023592401, 6033, 7},
		},
		{
			positions: []uint64{17570, 54607},
			a:         []uint64{1010, 71},
			b:         []uint64{25, 150},
		},
	}

	var builderA, builderB bsiBuilder
	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			if len(tst.a) != len(tst.b) || len(tst.a) != len(tst.positions) {
				t.Fatalf("Malformed test, a is %d, but b is %d", len(tst.a), len(tst.b))
			}
			sort.Sort(tst)

			for i := 0; i < len(tst.a); i++ {
				builderA.Insert(tst.positions[i], tst.a[i])
				builderB.Insert(tst.positions[i], tst.b[i])
			}

			dataA, dataB := builderA.Build(), builderB.Build()
			dataC := addBSI(dataA, dataB)
			// maps id to count
			results := make(map[uint64]uint64)
			dataC.pivotDescending(NewRow().Union(dataC...), 0, nil, nil, func(count uint64, ids ...uint64) {
				for _, id := range ids {
					results[id] = count
				}
			})

			for i, id := range tst.positions {
				if results[id] != tst.a[i]+tst.b[i] {
					t.Fatalf("value %d mismatch, id: %d. got %d, want %d", i, id, results[id], tst.a[i]+tst.b[i])
				}
			}
		})
	}
}
