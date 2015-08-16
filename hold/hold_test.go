package hold_test

import (
	"testing"
	"time"

	"github.com/umbel/pilosa/hold"
	"github.com/umbel/pilosa/util"
)

// Ensure hold can get a value that has been set.
func TestHold_Get(t *testing.T) {
	h := hold.NewHolder()
	go h.Run()

	id := util.RandomUUID()
	h.Set(&id, "derp", 10)
	if v, err := h.Get(&id, 10); err != nil {
		t.Fatal(err)
	} else if v != "derp" {
		t.Fatalf("unexpected value: %v", v)
	}
}

// Ensure hold can wait for a value that has not been set yet.
func TestHold_Get_Delay(t *testing.T) {
	h := hold.NewHolder()
	go h.Run()

	id := util.RandomUUID()
	go func() {
		time.Sleep(500 * time.Millisecond)
		h.Set(&id, "derpsy", 10)
	}()

	if v, err := h.Get(&id, 10); err != nil {
		t.Fatal(err)
	} else if v != "derpsy" {
		t.Fatalf("unexpected value: %v", v)
	}
}
