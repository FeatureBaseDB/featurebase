package index_test

import (
	"testing"

	"github.com/umbel/pilosa/index"
	"github.com/umbel/pilosa/util"
)

func init() {
	index.Backend = "memory"
}

// Ensure a fragment can be retrieved from the container.
func TestFragmentContainer_Get(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustClear(1)

	if bh, err := fc.Get(util.SUUID(1), 1234); err != nil {
		t.Fatal(err)
	} else if bh == 0 {
		t.Fatal("expected non-zero bitmap handle")
	}
}

// Ensure a bit can be set on a bitmap.
func TestFragmentContainer_SetBit(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustClear(1)

	// Set a bit on the bitmap.
	if changed, err := fc.SetBit(1, uint64(1234), 1, 0); err != nil {
		t.Fatal(err)
	} else if changed == false {
		t.Fatal("expected change")
	}

	// Set the same bit on the bitmap. No change should be indicated.
	if changed, err := fc.SetBit(1, uint64(1234), 1, 0); err != nil {
		t.Fatal(err)
	} else if changed == true {
		t.Fatal("expected no change")
	}
}

// Ensure the number of bits on a bitmap can be counted.
func TestFragmentContainer_Count(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, util.SUUID(1))

	// Set a bit on the bitmap.
	bi1 := uint64(1234)
	if changed, err := fc.SetBit(util.SUUID(1), bi1, 1, 0); err != nil {
		t.Fatal(err)
	} else if changed == false {
		t.Fatal("expected change")
	}

	// Verify that one bit is set.
	if bh, err := fc.Get(util.SUUID(1), bi1); err != nil {
		t.Fatal(err)
	} else if n, err := fc.Count(util.SUUID(1), bh); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}
}

// Ensure the bits in two bitmaps can be unioned.
func TestFragmentContainer_Union(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustSetBit(1, 1234, 1, 0)
	fc.MustSetBit(1, 4321, 65537, 0)

	// Union the handles together.
	if result, err := fc.Union(1, []index.BitmapHandle{fc.MustGet(1, 1234), fc.MustGet(1, 4321)}); err != nil {
		t.Fatal(err)
	} else if n := fc.MustCount(1, result); n != 2 {
		t.Fatalf("unexpected union bit count: %d", n)
	}
}

// Ensure unioning a bitmap with an empty bitmap returns a single bit count.
func TestFragmentContainer_Union_Empty(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustSetBit(1, 1234, 1, 0)

	// Union the handles together.
	if result, err := fc.Union(1, []index.BitmapHandle{fc.MustGet(1, 1234), fc.MustGet(1, 4321)}); err != nil {
		t.Fatal(err)
	} else if n := fc.MustCount(1, result); n != 1 {
		t.Fatalf("unexpected empty union bit count: %d", n)
	}
}

// Ensure the bits in two bitmaps can be intersected.
func TestFragmentContainer_Intersect(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustSetBit(1, 1234, 1, 0)
	fc.MustSetBit(1, 4321, 65537, 0)

	// Intersect the handles together.
	if result, err := fc.Intersect(1, []index.BitmapHandle{fc.MustGet(1, 1234), fc.MustGet(1, 4321)}); err != nil {
		t.Fatal(err)
	} else if n := fc.MustCount(1, result); n != 0 {
		t.Fatalf("unexpected intersect bit count: %d", n)
	}
}

// Ensure the bits in two bitmaps can be diffed.
func TestFragmentContainer_Difference(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustSetBit(1, 1234, 1, 0)
	fc.MustSetBit(1, 4321, 65537, 0)

	// Compute the difference between the handles.
	if result, err := fc.Difference(1, []index.BitmapHandle{fc.MustGet(1, 1234), fc.MustGet(1, 4321)}); err != nil {
		t.Fatal(err)
	} else if n := fc.MustCount(1, result); n != 1 {
		t.Fatalf("unexpected difference bit count: %s", err)
	}
}

// Ensure bitmaps can be marshaled and unmarshaled to bytes.
func TestFragmentContainer_Bytes(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustSetBit(1, 1234, 1, 0)

	// Count bits and marshal to bytes.
	beforeN := fc.MustCount(1, 1234)
	buf, err := fc.GetBytes(1, 1234)
	if err != nil {
		t.Fatal(err)
	}

	// Marshal bytes back to a bitmap and re-count.
	bh2, err := fc.FromBytes(1, buf)
	if err != nil {
		t.Fatal(err)
	}
	afterN := fc.MustCount(1, bh2)

	// Ensure the original bit count matches the new bitmap's bit count.
	if beforeN != afterN {
		t.Fatalf("unexpected bit count: before=%d, after=%d", beforeN, afterN)
	}
}

// Ensure an empty bitmap can be returned.
func TestFragmentContainer_Empty(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	bh, err := fc.Empty(1)
	if err != nil {
		t.Fatal(err)
	} else if n := fc.MustCount(1, bh); n != 0 {
		t.Fatalf("unexpected bit count: %d", n)
	}
}

// Ensure a list of bitmap handles can be returned.
func TestFragmentContainer_GetList(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)
	fc.MustSetBit(1, 1234, 1, 0)
	fc.MustSetBit(1, 4321, 65537, 0)

	a, err := fc.GetList(1, []uint64{1234, 4321, 789})
	if err != nil {
		t.Fatal(err)
	}

	// Compute the union to ensure they're the correct bitmaps.
	if res, err := fc.Union(1, a); err != nil {
		t.Fatal(err)
	} else if n := fc.MustCount(1, res); n != 2 {
		t.Fatalf("unexpected bit count: %d", n)
	}
}

// Ensure brand bitmaps can perform a small number of set bits.
func TestFragmentContainer_SetBit_Brand_Small(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "b.n", 0, 2)
	for i := uint64(0); i < 1000; i++ {
		fc.SetBit(2, 1029, i, 0)
	}
}

// Ensure the top n can be computed for a brand.
func TestFragmentContainer_TopN_Brand(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "b.n", 0, 2)

	// Set bits on the bitmap.
	fc.MustSetBit(2, uint64(1), 1, 2)
	fc.MustSetBit(2, uint64(1), 2, 2)
	fc.MustSetBit(2, uint64(1), 3, 2)
	fc.MustSetBit(2, uint64(2), 1, 2)
	fc.MustSetBit(2, uint64(2), 2, 2)
	fc.MustSetBit(2, uint64(3), 1, 2)

	// Retrieve the bitmap handle for bitmap 1.
	bh := fc.MustGet(2, uint64(1))

	// Compute the top-n.
	if results, err := fc.TopN(2, bh, 4, []uint64{2}); err != nil {
		t.Fatal(err)
	} else if results[0].Key != 1 {
		t.Fatalf("unexpected key: %d", results[0].Key)
	} else if results[0].Count != 3 {
		t.Fatalf("unexpected value: %d", results[0].Count)
	}
}

// Ensure a fragment can be cleared.
func TestFragmentContainer_Clear(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "general", 0, 1)

	// Compute the top-n.
	if res, err := fc.Clear(1); err != nil {
		t.Fatal(err)
	} else if res != true {
		t.Fatalf("unexpected result: %v", res)
	}
}

// Ensure a fragment can be marshaled and unmarshaled to and from bytes.
func TestFragmentContainer_FromBytes(t *testing.T) {
	fc := NewFragmentContainer()
	fc.AddFragment("25", "b.n", 0, 2)

	// Set bits for a bitmap.
	for i := 0; i < 4096; i++ {
		fc.MustSetBit(2, 1, uint64(i), 0)
	}

	// Marshal to bytes.
	buf, err := fc.GetBytes(2, fc.MustGet(2, 1))
	if err != nil {
		t.Fatal(err)
	}

	// Load a bitmap from compressed data.
	bh, err := fc.FromBytes(2, buf)
	if err != nil {
		t.Fatal(err)
	}

	// Load and count bits.
	if n := fc.MustCount(2, bh); n != 4096 {
		t.Fatalf("unexpected bit count: %d", n)
	}
}

// FragementContainer is a test wrapper for index.FragmentContainer.
type FragmentContainer struct {
	*index.FragmentContainer
}

// NewFragmentContainer returns a new instance of FragmentContainer.
func NewFragmentContainer() *FragmentContainer {
	return &FragmentContainer{index.NewFragmentContainer()}
}

// MustGet retrieves a bitmap by id. Panic on error.
func (fc *FragmentContainer) MustGet(frag_id util.SUUID, bitmap_id uint64) index.BitmapHandle {
	bh, err := fc.Get(frag_id, bitmap_id)
	if err != nil {
		panic(err)
	}
	return bh
}

// MustSetBit sets a bit in a bitmap. Panic on error.
func (fc *FragmentContainer) MustSetBit(frag_id util.SUUID, bitmap_id uint64, pos uint64, category uint64) bool {
	changed, err := fc.SetBit(frag_id, bitmap_id, pos, category)
	if err != nil {
		panic(err)
	}
	return changed
}

// MustClear clears a fragment. Panic on error.
func (fc *FragmentContainer) MustClear(fragmentID util.SUUID) bool {
	v, err := fc.Clear(fragmentID)
	if err != nil {
		panic(err)
	}
	return v
}

// MustCount returns the number of set bits in a bitmap. Panic on error.
func (fc *FragmentContainer) MustCount(frag_id util.SUUID, bitmap index.BitmapHandle) uint64 {
	v, err := fc.Count(frag_id, bitmap)
	if err != nil {
		panic(err)
	}
	return v
}
