package pilosa

import (
	"crypto/rand"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/testhook"
	bolt "go.etcd.io/bbolt"
)

func TestIDAlloc(t *testing.T) {
	// Acquire a temporary file.
	f, err := testhook.TempFile(t, "idalloc")
	if err != nil {
		t.Errorf("acquiring temporary file: %v", err)
		return
	}
	defer func() {
		cerr := f.Close()
		if cerr != nil {
			t.Errorf("closing temporary file: %v", cerr)
		}
	}()

	// Open bolt.
	db, err := bolt.Open(f.Name(), 0666, &bolt.Options{Timeout: 1 * time.Second, NoSync: true})
	if err != nil {
		t.Errorf("opening bolt: %v", err)
		return
	}
	defer func() {
		cerr := db.Close()
		if cerr != nil {
			t.Errorf("closing bolt: %v", cerr)
		}
	}()

	alloc := idAllocator{db: db}

	a := IDAllocKey{
		Index: "h",
		Key:   "a",
	}
	b := IDAllocKey{
		Index: "h",
		Key:   "b",
	}
	var sessions [11][32]byte
	for i := range sessions {
		_, err = io.ReadFull(rand.Reader, sessions[i][:])
		if err != nil {
			t.Errorf("failed to obtain entropy: %v", err)
			return
		}
	}

	// Reserve 2 batches of IDs and then commit both.
	if ranges, err := alloc.reserve(a, sessions[0], ^uint64(0), 3); err != nil {
		t.Errorf("failed to reserve 3 IDs: %v", err)
	} else {
		expect := []IDRange{{1, 3}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	if ranges, err := alloc.reserve(b, sessions[1], ^uint64(0), 3); err != nil {
		t.Errorf("failed to reserve 3 IDs: %v", err)
	} else {
		expect := []IDRange{{4, 6}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	if err := alloc.commit(a, sessions[0], 3); err != nil {
		t.Errorf("failed to commit reservation of 3 IDs: %v", err)
	}
	if err := alloc.commit(b, sessions[1], 3); err != nil {
		t.Errorf("failed to commit reservation of 3 IDs: %v", err)
	}

	// Test collision handling.
	if ranges, err := alloc.reserve(a, sessions[2], ^uint64(0), 4); err != nil {
		t.Errorf("failed to reserve 4 IDs: %v", err)
	} else {
		expect := []IDRange{{7, 10}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	if ranges, err := alloc.reserve(a, sessions[3], ^uint64(0), 4); err != nil {
		t.Errorf("failed to reserve 4 IDs: %v", err)
	} else {
		// Instead of reserving new ranges, we have taken over the previous reservation.
		expect := []IDRange{{7, 10}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	if err := alloc.commit(a, sessions[2], 4); err == nil {
		// This was taken over by reservation 3, so this should have warned us that something went wrong.
		t.Error("unexpected commit success for clobbered reservation")
	}
	if err := alloc.commit(a, sessions[3], 4); err != nil {
		t.Errorf("failed to commit reservation of 4 IDs: %v", err)
	}

	// Test key return.
	if ranges, err := alloc.reserve(a, sessions[4], ^uint64(0), 100); err != nil {
		t.Errorf("failed to reserve 100 IDs: %v", err)
	} else {
		// Instead of reserving new ranges, we have taken over the previous reservation.
		expect := []IDRange{{11, 110}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	if err := alloc.commit(a, sessions[4], 1); err != nil {
		t.Errorf("failed to commit reservation of 1 ID: %v", err)
	}
	if ranges, err := alloc.reserve(a, sessions[5], ^uint64(0), 100); err != nil {
		t.Errorf("failed to reserve 100 IDs: %v", err)
	} else {
		// Instead of reserving new ranges, we have taken over the previous reservation.
		expect := []IDRange{{12, 111}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	if err := alloc.commit(a, sessions[5], 100); err != nil {
		t.Errorf("failed to commit reservation of 100 IDs: %v", err)
	}

	// Test offset-based allocation.
	p1 := IDAllocKey{
		Index: "xyzzy",
		Key:   "kafka-partition-1",
	}
	p2 := IDAllocKey{
		Index: "xyzzy",
		Key:   "kafka-partition-2",
	}
	// Start processsing on 2 partitions.
	if ranges, err := alloc.reserve(p1, sessions[6], 0, 3); err != nil {
		t.Errorf("failed to reserve 3 IDs: %v", err)
	} else {
		expect := []IDRange{{1, 3}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	if ranges, err := alloc.reserve(p2, sessions[7], 0, 3); err != nil {
		t.Errorf("failed to reserve 3 IDs: %v", err)
	} else {
		expect := []IDRange{{4, 6}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	// Partition 2 finishes a batch of 2 IDs.
	if err := alloc.commit(p2, sessions[7], 2); err != nil {
		t.Errorf("failed to commit reservation of 2 IDs: %v", err)
	}
	// Partition 2 reserves more IDs.
	if ranges, err := alloc.reserve(p2, sessions[8], 2, 3); err != nil {
		t.Errorf("failed to reserve 3 IDs: %v", err)
	} else {
		// This could be coalesced here, but that is not true in the general case.
		expect := []IDRange{{6, 6}, {7, 8}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	// Partition 1 ingester failed and was restarted.
	if ranges, err := alloc.reserve(p1, sessions[9], 0, 3); err != nil {
		t.Errorf("failed to reserve 3 IDs: %v", err)
	} else {
		expect := []IDRange{{1, 3}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	// This time, the partition 1 ingester finishes the batch.
	// It commits the offset to Kafka but fails to commit to the ID allocator.
	// It is restarted at Kafka's next offset.
	if ranges, err := alloc.reserve(p1, sessions[10], 3, 4); err != nil {
		t.Errorf("failed to reserve 3 IDs: %v", err)
	} else {
		expect := []IDRange{{9, 12}}
		if !reflect.DeepEqual(expect, ranges) {
			t.Errorf("expeced %v but got %v", expect, ranges)
		}
	}
	// Partition 1 finishes a batch of 4 IDs.
	if err := alloc.commit(p1, sessions[10], 4); err != nil {
		t.Errorf("failed to commit reservation of 4 IDs: %v", err)
	}
}
