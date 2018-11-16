package pilosa_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pilosa/pilosa"
)

func TestTranslateFile_TranslateColumn(t *testing.T) {
	s := MustOpenTranslateFile()
	defer s.MustClose()

	// First translation should start id at zero.
	if ids, err := s.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Next translation on the same index should move to one.
	if ids, err := s.TranslateColumnsToUint64("IDX0", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{2}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Translation on a different index restarts at 0.
	if ids, err := s.TranslateColumnsToUint64("IDX1", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Ensure that string values can be looked up by ID.
	if value, err := s.TranslateColumnToString("IDX0", 2); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value: %s", value)
	}

	// Ensure that non-existent values return "".
	if value, err := s.TranslateColumnToString("IDX0", 1000); err != nil {
		t.Fatal(err)
	} else if value != "" {
		t.Fatalf("unexpected value: %s", value)
	}

	// Reopen the store.
	if err := s.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Ensure translation is still correct after reopen.
	if ids, err := s.TranslateColumnsToUint64("IDX1", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Ensure translation is still correct after reopen.
	if value, err := s.TranslateColumnToString("IDX0", 2); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value: %s", value)
	}

	// Next translation on the same index should move to one.
	if ids, err := s.TranslateColumnsToUint64("IDX0", []string{"baz"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{3}) {
		t.Fatalf("unexpected id: %#v", ids)
	}
}

func TestTranslateFile_TranslateColumn_Large(t *testing.T) {
	s := MustOpenTranslateFile()
	defer s.MustClose()

	// Generate key/values.
	for i := 0; i < 1000000; i += 1000 {
		keys := make([]string, 1000)
		for j := 0; j < 1000; j++ {
			keys[j] = strconv.Itoa(i + j + 1)
		}

		ids, err := s.TranslateColumnsToUint64("IDX0", keys)
		if err != nil {
			t.Fatal(err)
		}

		for j, id := range ids {
			if exp := uint64(i + j + 1); id != exp {
				t.Fatalf("unexpected id: got=%d, exp=%d", id, exp)
			}
		}
	}

	// Verify values can be returned.
	for i := 0; i < 1000000; i++ {
		exp := strconv.Itoa(i + 1)
		if key, err := s.TranslateColumnToString("IDX0", uint64(i+1)); err != nil {
			t.Fatal(err)
		} else if key != exp {
			t.Fatalf("unexpected key: got=%q, exp=%q", key, exp)
		}
	}

	// Reopen and re-verify.
	if err := s.Reopen(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000000; i++ {
		exp := strconv.Itoa(i + 1)
		if key, err := s.TranslateColumnToString("IDX0", uint64(i+1)); err != nil {
			t.Fatal(err)
		} else if key != exp {
			t.Fatalf("unexpected key: got=%q, exp=%q", key, exp)
		}
	}
}

func TestTranslateFile_TranslateRow(t *testing.T) {
	s := MustOpenTranslateFile()
	defer s.MustClose()

	// First translation should start id at zero.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FIELD0", []string{"foo"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Next translation on the same index should move to one.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FIELD0", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{2}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Translation on a different index restarts at 0.
	if ids, err := s.TranslateRowsToUint64("IDX1", "FIELD0", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Translation on a different field restarts at 0.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FIELD1", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Ensure that string values can be looked up by ID.
	if value, err := s.TranslateRowToString("IDX0", "FIELD0", 2); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value: %s", value)
	}

	// Ensure that non-existent values return blank.
	if value, err := s.TranslateRowToString("IDX0", "FIELD0", 1000); err != nil {
		t.Fatal(err)
	} else if value != "" {
		t.Fatalf("unexpected value: %s", value)
	}

	// Reopen the store.
	if err := s.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Translation on a different field restarts at 0.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FIELD1", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Ensure that string values can be looked up by ID.
	if value, err := s.TranslateRowToString("IDX0", "FIELD0", 2); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value: %s", value)
	}

	// Translate new row and increment sequence.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FIELD0", []string{"baz"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{3}) {
		t.Fatalf("unexpected id: %#v", ids)
	}
}

func TestTranslateFile_TranslateRow_Large(t *testing.T) {
	s := MustOpenTranslateFile()
	defer s.MustClose()

	// Generate key/values.
	for i := 0; i < 1000000; i += 1000 {
		keys := make([]string, 1000)
		for j := 0; j < 1000; j++ {
			keys[j] = strconv.Itoa(i + j + 1)
		}

		ids, err := s.TranslateRowsToUint64("IDX0", "FIELD0", keys)
		if err != nil {
			t.Fatal(err)
		}

		for j, id := range ids {
			if exp := uint64(i + j + 1); id != exp {
				t.Fatalf("unexpected id: got=%d, exp=%d", id, exp)
			}
		}
	}

	// Verify values can be returned.
	for i := 0; i < 1000000; i++ {
		exp := strconv.Itoa(i + 1)
		if key, err := s.TranslateRowToString("IDX0", "FIELD0", uint64(i+1)); err != nil {
			t.Fatal(err)
		} else if key != exp {
			t.Fatalf("unexpected key: got=%q, exp=%q", key, exp)
		}
	}

	// Reopen and re-verify.
	if err := s.Reopen(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000000; i++ {
		exp := strconv.Itoa(i + 1)
		if key, err := s.TranslateRowToString("IDX0", "FIELD0", uint64(i+1)); err != nil {
			t.Fatal(err)
		} else if key != exp {
			t.Fatalf("unexpected key: got=%q, exp=%q", key, exp)
		}
	}
}

func TestTranslateFile_Reader(t *testing.T) {
	t.Run("NoOffset", func(t *testing.T) {
		s := MustOpenTranslateFile()
		defer s.MustClose()
		if _, err := s.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
			t.Fatal(err)
		} else if _, err := s.TranslateRowsToUint64("IDX0", "FIELD0", []string{"bar", "baz"}); err != nil {
			t.Fatal(err)
		}

		rc, err := s.Reader(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		brc := bufio.NewReader(rc)
		defer rc.Close()

		// Read first entry. Should read 'entry length' (13) plus uvarint(size) (1) = 14b.
		var entry pilosa.LogEntry
		if n, err := entry.ReadFrom(brc); err != nil {
			t.Fatal(err)
		} else if n != 14 {
			t.Fatalf("unexpected n: %d", n)
		} else if diff := cmp.Diff(entry, pilosa.LogEntry{
			Type:   pilosa.LogEntryTypeInsertColumn,
			Index:  []byte("IDX0"),
			IDs:    []uint64{1},
			Keys:   [][]byte{[]byte("foo")},
			Length: 13,
		}); diff != "" {
			t.Fatal(diff)
		}

		// Read second entry.
		if _, err := entry.ReadFrom(brc); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(entry, pilosa.LogEntry{
			Type:   pilosa.LogEntryTypeInsertRow,
			Index:  []byte("IDX0"),
			Field:  []byte("FIELD0"),
			IDs:    []uint64{1, 2},
			Keys:   [][]byte{[]byte("bar"), []byte("baz")},
			Length: 24,
		}); diff != "" {
			t.Fatal(diff)
		}

		// Write new entry.
		if _, err := s.TranslateColumnsToUint64("IDX0", []string{"xyz"}); err != nil {
			t.Fatal(err)
		}

		// Read new entry.
		if _, err := entry.ReadFrom(brc); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(entry, pilosa.LogEntry{
			Type:   pilosa.LogEntryTypeInsertColumn,
			Index:  []byte("IDX0"),
			IDs:    []uint64{2},
			Keys:   [][]byte{[]byte("xyz")},
			Length: 13,
		}); diff != "" {
			t.Fatal(diff)
		}

		// Close reader and ensure it returns EOF.
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		} else if _, err := entry.ReadFrom(brc); err != pilosa.ErrTranslateStoreReaderClosed {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("WithOffset", func(t *testing.T) {
		s := MustOpenTranslateFile()
		defer s.MustClose()
		if _, err := s.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
			t.Fatal(err)
		} else if _, err := s.TranslateRowsToUint64("IDX0", "FIELD0", []string{"bar", "baz"}); err != nil {
			t.Fatal(err)
		}

		// Start offset after the first entry.
		rc, err := s.Reader(context.Background(), 14)
		if err != nil {
			t.Fatal(err)
		}
		brc := bufio.NewReader(rc)
		defer rc.Close()

		// This should be the second entry.
		var entry pilosa.LogEntry
		if _, err := entry.ReadFrom(brc); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(entry, pilosa.LogEntry{
			Type:   pilosa.LogEntryTypeInsertRow,
			Index:  []byte("IDX0"),
			Field:  []byte("FIELD0"),
			IDs:    []uint64{1, 2},
			Keys:   [][]byte{[]byte("bar"), []byte("baz")},
			Length: 24,
		}); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPrintTranslateFile(t *testing.T) {
	// I think this is related to the mmap in s.Open.
	t.Skip("causes fatal error: fault")
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	f.Close()

	s := pilosa.NewTranslateFile(pilosa.OptTranslateFileMapSize(2 << 25))
	s.Path = f.Name()
	err = s.Open()
	if err != nil {
		t.Fatalf("opening : %v", err)
	}
	fmt.Println("blah ", s)
}

func TestTranslateFile_PrimaryTranslateStore(t *testing.T) {
	// Create a primary store that accepts writes.
	primary := MustOpenTranslateFile()
	defer primary.MustClose()

	// Create a replica that accepts writes from primary.
	replica := NewTranslateFile()
	replica.SetPrimaryStore("primary", primary)
	if err := replica.Open(); err != nil {
		t.Fatal(err)
	}
	defer replica.MustClose()

	// Write to the primary.
	if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
		t.Fatal(err)
	} else if _, err := primary.TranslateRowsToUint64("IDX0", "FIELD0", []string{"bar", "baz"}); err != nil {
		t.Fatal(err)
	}

	// Attempt to read replica until writes appear.
	if err := retryFor(2*time.Second, func() error {
		// Verify that replica has received writes.
		if value, err := replica.TranslateColumnToString("IDX0", 1); err != nil {
			return err
		} else if value != "foo" {
			return fmt.Errorf("unexpected column 1 value: %s", value)
		}

		if value, err := replica.TranslateRowToString("IDX0", "FIELD0", 1); err != nil {
			return err
		} else if value != "bar" {
			return fmt.Errorf("unexpected row 1 value: %s", value)
		}

		if value, err := replica.TranslateRowToString("IDX0", "FIELD0", 2); err != nil {
			return err
		} else if value != "baz" {
			return fmt.Errorf("unexpected row 2 value: %s", value)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Disconnect primary store & write more values.
	if err := primary.Reopen(); err != nil {
		t.Fatal(err)
	} else if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"baz"}); err != nil {
		t.Fatal(err)
	}

	// Attempt to read replica until writes appear.
	if err := retryFor(2*time.Second, func() error {
		if value, err := replica.TranslateColumnToString("IDX0", 2); err != nil {
			return err
		} else if value != "baz" {
			return fmt.Errorf("unexpected column 2 value: %s", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Disconnect replica store & write more values.
	if err := replica.Reopen(); err != nil {
		t.Fatal(err)
	} else if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"foobar"}); err != nil {
		t.Fatal(err)
	}

	// Attempt to read replica until writes appear.
	if err := retryFor(2*time.Second, func() error {
		if value, err := replica.TranslateColumnToString("IDX0", 3); err != nil {
			return err
		} else if value != "foobar" {
			return fmt.Errorf("unexpected column 3 value: %s", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTranslateFile_ReassignPrimaryTranslateStore(t *testing.T) {
	t.Run("AddNode", func(t *testing.T) {
		// Create a primary store that accepts writes.
		primary := MustOpenTranslateFile()
		defer primary.MustClose()

		// Create replica1 that accepts writes from primary.
		replica1 := NewTranslateFile()
		replica1.SetPrimaryStore("primary", primary)
		if err := replica1.Open(); err != nil {
			t.Fatal(err)
		}
		defer replica1.MustClose()

		// Write to the primary.
		if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
			t.Fatal(err)
		} else if _, err := primary.TranslateRowsToUint64("IDX0", "FIELD0", []string{"bar", "baz"}); err != nil {
			t.Fatal(err)
		}

		// Attempt to read replica1 until writes appear.
		if err := retryFor(2*time.Second, func() error {
			// Verify that replica1 has received writes.
			if value, err := replica1.TranslateColumnToString("IDX0", 1); err != nil {
				return err
			} else if value != "foo" {
				return fmt.Errorf("unexpected column 1 value: %s", value)
			}

			if value, err := replica1.TranslateRowToString("IDX0", "FIELD0", 1); err != nil {
				return err
			} else if value != "bar" {
				return fmt.Errorf("unexpected row 1 value: %s", value)
			}

			if value, err := replica1.TranslateRowToString("IDX0", "FIELD0", 2); err != nil {
				return err
			} else if value != "baz" {
				return fmt.Errorf("unexpected row 2 value: %s", value)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Create replica2 that accepts writes from primary,
		// and change replica1's primary to be replica2.
		// From: P <- R1
		//   To: P <- R2 <- R1
		// Momentarily, replica1 should be ahead of replica2, so we will see log
		// messages like "translate store reader past file size: sz=0 off=39"
		// But eventually it should get in sync and new writes will be available
		// on replica1.
		replica2 := NewTranslateFile()
		replica2.SetPrimaryStore("primary", primary)
		replica1.SetPrimaryStore("replica2", replica2)

		if err := replica2.Open(); err != nil {
			t.Fatal(err)
		}
		defer replica2.MustClose()

		// Attempt to read replica2 until writes appear.
		if err := retryFor(2*time.Second, func() error {
			// Verify that replica2 have received writes.
			if value, err := replica2.TranslateColumnToString("IDX0", 1); err != nil {
				return err
			} else if value != "foo" {
				return fmt.Errorf("unexpected column 1 value: %s", value)
			}

			if value, err := replica2.TranslateRowToString("IDX0", "FIELD0", 1); err != nil {
				return err
			} else if value != "bar" {
				return fmt.Errorf("unexpected row 1 value: %s", value)
			}

			if value, err := replica2.TranslateRowToString("IDX0", "FIELD0", 2); err != nil {
				return err
			} else if value != "baz" {
				return fmt.Errorf("unexpected row 2 value: %s", value)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Add more data to the primary and ensure that replica1 receives the
		// data (via replica2).
		if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"baz"}); err != nil {
			t.Fatal(err)
		}

		// Attempt to read replica1 until writes appear.
		if err := retryFor(2*time.Second, func() error {
			if value, err := replica1.TranslateColumnToString("IDX0", 2); err != nil {
				return err
			} else if value != "baz" {
				return fmt.Errorf("unexpected column 2 value: %s", value)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("RemoveNode", func(t *testing.T) {
		// Create a primary store that accepts writes.
		primary := MustOpenTranslateFile()
		defer primary.MustClose()

		// Create two replicas that accepts writes from the primary
		// in a daisy-chain configuration.
		// P <- R1 <- R2

		// Create replica1.
		replica1 := NewTranslateFile()
		replica1.SetPrimaryStore("primary", primary)
		if err := replica1.Open(); err != nil {
			t.Fatal(err)
		}
		defer replica1.MustClose()

		// Create replica2.
		replica2 := NewTranslateFile()
		replica2.SetPrimaryStore("replica1", replica1)
		if err := replica2.Open(); err != nil {
			t.Fatal(err)
		}
		defer replica2.MustClose()

		// Write to the primary.
		if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
			t.Fatal(err)
		} else if _, err := primary.TranslateRowsToUint64("IDX0", "FIELD0", []string{"bar", "baz"}); err != nil {
			t.Fatal(err)
		}

		// Attempt to read replica2 until writes appear.
		if err := retryFor(2*time.Second, func() error {
			// Verify that replica2 has received writes.
			if value, err := replica2.TranslateColumnToString("IDX0", 1); err != nil {
				return err
			} else if value != "foo" {
				return fmt.Errorf("unexpected column 1 value: %s", value)
			}

			if value, err := replica2.TranslateRowToString("IDX0", "FIELD0", 1); err != nil {
				return err
			} else if value != "bar" {
				return fmt.Errorf("unexpected row 1 value: %s", value)
			}

			if value, err := replica2.TranslateRowToString("IDX0", "FIELD0", 2); err != nil {
				return err
			} else if value != "baz" {
				return fmt.Errorf("unexpected row 2 value: %s", value)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Remove replica1 from the replication chain.
		// From: P <- R1 <- R2
		//   To: P <- R2
		replica1.SetPrimaryStore("", nil)

		// Add more data to the primary and ensure that replica2 receives the
		// data (after replica1 is removed).
		if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"baz"}); err != nil {
			t.Fatal(err)
		}

		// Set replica2 to replicate from primary.
		replica2.SetPrimaryStore("primary", primary)

		// Attempt to read replica2 until writes appear.
		if err := retryFor(2*time.Second, func() error {
			if value, err := replica2.TranslateColumnToString("IDX0", 2); err != nil {
				return err
			} else if value != "baz" {
				return fmt.Errorf("unexpected column 2 value: %s", value)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ChangePrimaryWriter", func(t *testing.T) {
		// Create a primary store that accepts writes.
		primary := MustOpenTranslateFile()
		defer primary.MustClose()

		// Create two replicas that accepts writes from the primary
		// in a daisy-chain configuration.
		// P <- R1 <- R2

		// Create replica1.
		replica1 := NewTranslateFile()
		replica1.SetPrimaryStore("primary", primary)
		if err := replica1.Open(); err != nil {
			t.Fatal(err)
		}
		defer replica1.MustClose()

		// Create replica2.
		replica2 := NewTranslateFile()
		replica2.SetPrimaryStore("replica1", replica1)
		if err := replica2.Open(); err != nil {
			t.Fatal(err)
		}
		defer replica2.MustClose()

		// Write to the primary.
		if _, err := primary.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
			t.Fatal(err)
		} else if _, err := primary.TranslateRowsToUint64("IDX0", "FIELD0", []string{"bar", "baz"}); err != nil {
			t.Fatal(err)
		}

		// Attempt to read replica2 until writes appear.
		if err := retryFor(2*time.Second, func() error {
			// Verify that replica2 has received writes.
			if value, err := replica2.TranslateColumnToString("IDX0", 1); err != nil {
				return err
			} else if value != "foo" {
				return fmt.Errorf("unexpected column 1 value: %s", value)
			}

			if value, err := replica2.TranslateRowToString("IDX0", "FIELD0", 1); err != nil {
				return err
			} else if value != "bar" {
				return fmt.Errorf("unexpected row 1 value: %s", value)
			}

			if value, err := replica2.TranslateRowToString("IDX0", "FIELD0", 2); err != nil {
				return err
			} else if value != "baz" {
				return fmt.Errorf("unexpected row 2 value: %s", value)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Change replica1 to be the primary writer.
		// From: P <- R1 <- R2
		//   To: R1 <- R2 <- P
		replica1.SetPrimaryStore("", nil)

		// SetPrimaryStore is asynchronous, so we need to wait before writing new data.
		time.Sleep(200 * time.Millisecond)

		// Add more data to the primary (now replica1) and ensure that primary (now a read-only replica)
		// receives the data.
		if _, err := replica1.TranslateColumnsToUint64("IDX0", []string{"baz"}); err != nil {
			t.Fatal(err)
		}

		// Set primary to replicate from replica2.
		primary.SetPrimaryStore("replica2", replica2)

		// Attempt to read primary until writes appear.
		if err := retryFor(2*time.Second, func() error {
			if value, err := primary.TranslateColumnToString("IDX0", 2); err != nil {
				return err
			} else if value != "baz" {
				return fmt.Errorf("unexpected column 2 value: %s", value)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkTranslateFile_TranslateColumnsToUint64(b *testing.B) {
	const batchSize = 1000

	s := MustOpenTranslateFile()
	defer s.MustClose()

	// Generate keys before benchmark begins
	keySets := make([][]string, b.N/batchSize)
	for i := range keySets {
		keySets[i] = make([]string, batchSize)
		for j, jv := range rand.New(rand.NewSource(0)).Perm(batchSize) {
			keySets[i][j] = fmt.Sprintf("%08d%08d", jv, i)
		}
	}

	b.ResetTimer()

	for _, keySet := range keySets {
		if _, err := s.TranslateColumnsToUint64("IDX0", keySet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTranslateFile_TranslateColumnToString(b *testing.B) {
	const batchSize = 1000

	s := MustOpenTranslateFile()
	defer s.MustClose()

	// Generate keys before benchmark begins
	for i := 0; i < b.N; i += batchSize {
		keySet := make([]string, batchSize)
		for j, jv := range rand.New(rand.NewSource(0)).Perm(batchSize) {
			keySet[j] = fmt.Sprintf("%08d%08d", jv, i)
		}
		if _, err := s.TranslateColumnsToUint64("IDX0", keySet); err != nil {
			b.Fatal(err)
		}
	}

	// Generate random key access.
	perm := rand.New(rand.NewSource(0)).Perm(b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := s.TranslateColumnToString("IDX0", uint64(perm[i])); err != nil {
			b.Fatal(err)
		}
	}
}

type TranslateFile struct {
	lock sync.Mutex
	*pilosa.TranslateFile
}

func NewTranslateFile() *TranslateFile {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	f.Close()

	s := &TranslateFile{TranslateFile: pilosa.NewTranslateFile(pilosa.OptTranslateFileMapSize(2 << 26))}
	s.Path = f.Name()
	return s
}

func (t *TranslateFile) Reader(ctx context.Context, offset int64) (io.ReadCloser, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.TranslateFile.Reader(ctx, offset)
}

func MustOpenTranslateFile() *TranslateFile {
	s := NewTranslateFile()
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

func (s *TranslateFile) Close() error {
	defer os.Remove(s.Path)
	return s.TranslateFile.Close()
}

func (s *TranslateFile) MustClose() {
	if err := s.Close(); err != nil {
		panic(err)
	}
}

// Reopen closes the store and opens a new instance of it for the same path.
func (s *TranslateFile) Reopen() error {
	prev := s.TranslateFile
	if err := s.TranslateFile.Close(); err != nil {
		return err
	}

	s.lock.Lock()
	s.TranslateFile = pilosa.NewTranslateFile(pilosa.OptTranslateFileMapSize(2 << 25))
	s.lock.Unlock()
	s.Path = prev.Path
	s.SetPrimaryStore("restored-primary", prev.PrimaryTranslateStore)
	return s.Open()
}

// retryFor executes fn every 100ms until d time passes or until fn return nil.
func retryFor(d time.Duration, fn func() error) (err error) {
	timer, ticker := time.NewTimer(d), time.NewTicker(100*time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()

	for {
		if err = fn(); err == nil {
			return nil
		}

		select {
		case <-timer.C:
			return err
		case <-ticker.C:
		}
	}
}
