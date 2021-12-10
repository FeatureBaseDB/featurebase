package roaring

import (
	"fmt"
	"testing"
)

type containerOp struct {
	name string
	fn   func(a, b *Container)
}

var containerOps = []containerOp{
	{"intersect", func(a, b *Container) { _ = intersect(a, b) }},
	{"union", func(a, b *Container) { _ = union(a, b) }},
	{"difference", func(a, b *Container) { _ = difference(a, b) }},
	{"xor", func(a, b *Container) { _ = xor(a, b) }},
	{"intersectionCount", func(a, b *Container) { _ = intersectionCount(a, b) }},
}

// Run each container type against each other container type. In an earlier
// implementation, this had a subtle bug; we generated two sets of archetypal
// containers, so the run of Array1 vs. Array4096 used list1's Array1, and
// list2's Array4096, and the run of Arary4096 vs Array1 used list1's Array4096
// and list2's Array1. This created a subtle performance glitch, because
// list1 happened to have an Array1 containing 53,127 and list2 happened to
// have an Array1 containing 3,917, which meant that the second item being
// Array1 often looked dramatically faster than the first item being Array1.
// To reduce the impact of such things, we generate 8 of each container, and
// do each test on the whole 8x8 matrix. This does mean each operation is
// being run with a container compared with itself 1/8 of the time.
func BenchmarkCtOps(b *testing.B) {
	ca, err := InitContainerArchetypes()
	if err != nil {
		b.Fatalf("creating container archetypes: %v", err)
	}
	for idx1, n1 := range ContainerArchetypeNames {
		ca1 := ca[idx1]
		for idx2, n2 := range ContainerArchetypeNames {
			base := fmt.Sprintf("%s/%s", n1, n2)
			ca2 := ca[idx2]
			b.Run(base, func(b *testing.B) {
				for _, op := range containerOps {
					b.Run(op.name, func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							for _, c1 := range ca1 {
								for _, c2 := range ca2 {
									op.fn(c1, c2)
								}
							}
						}
					})
				}
			})
		}
	}
}

func TestIntersectVariants(t *testing.T) {
	ca, err := InitContainerArchetypes()
	if err != nil {
		t.Fatalf("creating container archetypes: %v", err)
	}
	for idx1, n1 := range ContainerArchetypeNames {
		ca1 := ca[idx1]
		for idx2, n2 := range ContainerArchetypeNames {
			ca2 := ca[idx2]
			for i1, c1 := range ca1 {
				for i2, c2 := range ca2 {
					full := intersect(c1, c2)
					count := intersectionCount(c1, c2)
					if full.N() != count {
						t.Errorf("intersecting %s[%d] and %s[%d]: container has N %d, count was %d",
							n1, i1, n2, i2, full.N(), count)
					}
					any := intersectionAny(c1, c2)
					if any != (count != 0) {
						t.Errorf("intersecting %s[%d] and %s[%d]: any %t, count was %d",
							n1, i1, n2, i2, any, count)
					}
				}
			}
		}
	}
}

func TestIntersectionAnyRunBitmapSingleWordRegression(t *testing.T) {
	// In a previous version, single-word runs would match any bit within the word.
	// Verify that this no longer happens.
	any := intersectionAnyRunBitmap(
		NewContainerRun([]Interval16{{1, 2}}),
		NewContainerBitmapN([]uint64{0b1001}, 2),
	)
	if any {
		t.Errorf("matched an exclusive single-word run")
	}
}
