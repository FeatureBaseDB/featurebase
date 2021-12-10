// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/test"
	"github.com/molecula/featurebase/v2/testhook"
	"github.com/pkg/errors"
)

// ShardWidth is a helper reference to use when testing.
const ShardWidth = pilosa.ShardWidth

// Ensure index can open and retrieve a field.
func TestIndex_CreateFieldIfNotExists(t *testing.T) {
	index := test.MustOpenIndex(t)
	defer index.Close()

	// Create field.
	f, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	} else if f == nil {
		t.Fatal("expected field")
	}

	// Retrieve existing field.
	other, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	} else if f.Field != other.Field {
		t.Fatal("field mismatch")
	}

	if f.Field != index.Field("f") {
		t.Fatal("field mismatch")
	}
}

func TestIndex_CreateField(t *testing.T) {
	// Ensure time quantum can be set appropriately on a new field.
	t.Run("TimeQuantum", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			index := test.MustOpenIndex(t)
			defer index.Close()

			// Create field with explicit quantum.
			f, err := index.CreateField("f", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))
			if err != nil {
				t.Fatal(err)
			} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
				t.Fatalf("unexpected field time quantum: %s", q)
			}
		})
	})

	// Ensure time quantum can be set appropriately on a new field.
	t.Run("TimeQuantumNoStandardView", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			index := test.MustOpenIndex(t)
			defer index.Close()

			// Create field with explicit quantum with no standard view
			f, err := index.CreateField("f", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH"), true))
			if err != nil {
				t.Fatal(err)
			} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
				t.Fatalf("unexpected field time quantum: %s", q)
			}
		})
	})

	// Ensure field can include range columns.
	t.Run("BSIFields", func(t *testing.T) {
		t.Run("Int", func(t *testing.T) {
			index := test.MustOpenIndex(t)
			defer index.Close()

			// Create field with schema and verify it exists.
			if f, err := index.CreateField("f", pilosa.OptFieldTypeInt(-990, 1000)); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(f.Type(), pilosa.FieldTypeInt) {
				t.Fatalf("unexpected type: %#v", f.Type())
			}

			// Reopen the index & verify the fields are loaded.
			if err := index.Reopen(); err != nil {
				t.Fatal(err)
			} else if f := index.Field("f"); !reflect.DeepEqual(f.Type(), pilosa.FieldTypeInt) {
				t.Fatalf("unexpected type after reopen: %#v", f.Type())
			}
		})

		t.Run("Timestamp", func(t *testing.T) {
			index := test.MustOpenIndex(t)
			defer index.Close()

			// Create field with schema and verify it exists.
			if f, err := index.CreateField("f", pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds)); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(f.Type(), pilosa.FieldTypeTimestamp) {
				t.Fatalf("unexpected type: %#v", f.Type())
			}

			// Reopen the index & verify the fields are loaded.
			if err := index.Reopen(); err != nil {
				t.Fatal(err)
			} else if f := index.Field("f"); !reflect.DeepEqual(f.Type(), pilosa.FieldTypeTimestamp) {
				t.Fatalf("unexpected type after reopen: %#v", f.Type())
			}
		})

		// TODO: These errors don't apply  here. Instead, we need these tests
		// on field creation FieldOptions validation.
		/*
			t.Run("ErrRangeCacheAllowed", func(t *testing.T) {
				index := test.MustOpenIndex(t)
				defer index.Close()

				if _, err := index.CreateField("f", pilosa.FieldOptions{
					CacheType: pilosa.CacheTypeRanked,
				}); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("BSIFieldsWithCacheTypeNone", func(t *testing.T) {
				index := test.MustOpenIndex(t)
				defer index.Close()
				if _, err := index.CreateField("f", pilosa.FieldOptions{
					CacheType: pilosa.CacheTypeNone,
					CacheSize: uint32(5),
				}); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("ErrFieldFieldsAllowed", func(t *testing.T) {
				index := test.MustOpenIndex(t)
				defer index.Close()

				if _, err := index.CreateField("f", pilosa.FieldOptions{
					Fields: []*pilosa.Field{
						{Name: "field0", Type: pilosa.FieldTypeInt},
					},
				}); err != nil {
					t.Fatal(err)
				}
			})

			t.Run("ErrFieldNameRequired", func(t *testing.T) {
				index := test.MustOpenIndex(t)
				defer index.Close()

				if _, err := index.CreateField("f", pilosa.FieldOptions{
					Fields: []*pilosa.Field{
						{Name: "", Type: pilosa.FieldTypeInt},
					},
				}); err != pilosa.ErrFieldNameRequired {
					t.Fatal(err)
				}
			})

			t.Run("ErrInvalidFieldType", func(t *testing.T) {
				index := test.MustOpenIndex(t)
				defer index.Close()

				if _, err := index.CreateField("f", pilosa.FieldOptions{
					Fields: []*pilosa.Field{
						{Name: "field0", Type: "bad_type"},
					},
				}); err != pilosa.ErrInvalidFieldType {
					t.Fatal(err)
				}
			})

			t.Run("ErrInvalidBSIGroupRange", func(t *testing.T) {
				index := test.MustOpenIndex(t)
				defer index.Close()

				if _, err := index.CreateField("f", pilosa.FieldOptions{
					Fields: []*pilosa.Field{
						{Name: "field0", Type: pilosa.FieldTypeInt, Min: 100, Max: 50},
					},
				}); err != pilosa.ErrInvalidBSIGroupRange {
					t.Fatal(err)
				}
			})
		*/
	})

	t.Run("WithKeys", func(t *testing.T) {
		// Don't allow an int field to be created with keys=true
		t.Run("IntField", func(t *testing.T) {
			index := test.MustOpenIndex(t)
			defer index.Close()

			_, err := index.CreateField("f", pilosa.OptFieldTypeInt(-1, 1), pilosa.OptFieldKeys())
			if errors.Cause(err) != pilosa.ErrIntFieldWithKeys {
				t.Fatal("int field cannot be created with keys=true")
			}
		})

		// Don't allow a decimal field to be created with keys=true
		t.Run("DecimalField", func(t *testing.T) {
			index := test.MustOpenIndex(t)
			defer index.Close()

			_, err := index.CreateField("f", pilosa.OptFieldTypeDecimal(1, pql.Decimal{Value: -1}, pql.Decimal{Value: 1}), pilosa.OptFieldKeys())
			if errors.Cause(err) != pilosa.ErrDecimalFieldWithKeys {
				t.Fatal("decimal field cannot be created with keys=true")
			}
		})
	})
}

// Ensure index can delete a field.
func TestIndex_DeleteField(t *testing.T) {
	index := test.MustOpenIndex(t)
	defer index.Close()

	// Create field.
	if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	// Delete field & verify it's gone.
	if err := index.DeleteField("f"); err != nil {
		t.Fatal(err)
	} else if index.Field("f") != nil {
		t.Fatal("expected nil field")
	}

	// Delete again to make sure it errors.
	err := index.DeleteField("f")
	if !isNotFoundError(err) {
		t.Fatalf("expected 'field not found' error, got: %#v", err)
	}
}

// Ensure index can validate its name.
func TestIndex_InvalidName(t *testing.T) {
	path, err := testhook.TempDir(t, "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := pilosa.NewIndex(pilosa.NewHolder(path, mustHolderConfig()), path, "ABC")
	if err == nil {
		t.Fatalf("should have gotten an error on index name with caps")
	}
	if index != nil {
		t.Fatalf("unexpected index name %v", index)
	}
}

func isNotFoundError(err error) bool {
	root := errors.Cause(err)
	_, ok := root.(pilosa.NotFoundError)
	return ok
}

// Ensure that after node/cluster restart, deleting and recreating a field
// does not cause a deadlock
// This is a regression test after a customer experienced the same deadlock.
// For details, check out https://molecula.atlassian.net/browse/CORE-919
func TestIndex_RecreateFieldOnRestart(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	// create index
	indexName := fmt.Sprintf("idx_%d", rand.Uint64())
	holder := c.GetHolder(0)
	index, err := holder.CreateIndex(indexName, pilosa.IndexOptions{
		Keys: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	// create field
	fieldName := fmt.Sprintf("field_%d", rand.Uint64())
	_, err = c.GetNode(0).API.CreateField(context.Background(), indexName, fieldName,
		pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	}

	// set value
	_, err = c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: indexName,
		Query: fmt.Sprintf(`Set(1, %s=1)`, fieldName),
	})
	if err != nil {
		t.Fatal(err)
	}

	// restart node
	node := c.GetNode(0)
	if err := node.Reopen(); err != nil {
		t.Fatal(err)
	}
	if err := c.AwaitState(disco.ClusterStateNormal, 10*time.Second); err != nil {
		t.Fatalf("restarting cluster: %v", err)
	}

	// delete field
	err = c.GetNode(0).API.DeleteField(context.Background(), indexName, fieldName)
	if err != nil {
		t.Fatal(err)
	}

	// recreate field
	errCh := make(chan error)
	go func() {
		_, err := c.GetNode(0).API.CreateField(context.Background(), indexName,
			fieldName, pilosa.OptFieldTypeDefault())
		errCh <- err
	}()
	select {
	case <-time.After(10 * time.Second):
		// We have to use os.Exit here instead of t.Fatal or panic since
		// on panic, deferred statements are still ran. Given that
		// we have deferred cluster.Close(), it deadlocks on the same
		// issue this test is, well, is testing on.
		// With os.Exit, the process exits at that point without running the
		// deferred actions. This is more of a work-around fix to make the
		// test meaningful on timeout.
		t.Logf("recreating field took too long")
		os.Exit(1)
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	}

}
