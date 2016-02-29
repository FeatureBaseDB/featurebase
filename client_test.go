package pilosa_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/umbel/pilosa"
)

// Ensure client can bulk import data.
func TestClient_Import(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Index = idx.Index

	// Send import request.
	c := MustNewClient(s.Host())
	if err := c.Import("d", "f", 0, []pilosa.Bit{
		{BitmapID: 0, ProfileID: 1},
		{BitmapID: 0, ProfileID: 5},
		{BitmapID: 200, ProfileID: 6},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	f := idx.MustCreateFragmentIfNotExists("d", "f", 0)
	if a := f.Bitmap(0).Bits(); !reflect.DeepEqual(a, []uint64{1, 5}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
	if a := f.Bitmap(200).Bits(); !reflect.DeepEqual(a, []uint64{6}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Ensure client backup and restore a frame.
func TestClient_BackupRestore(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	idx.MustCreateFragmentIfNotExists("d", "f", 0).MustSetBits(100, 1, 2, 3, SliceWidth-1)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(100, SliceWidth, SliceWidth+2)
	idx.MustCreateFragmentIfNotExists("d", "f", 5).MustSetBits(100, (5*SliceWidth)+1)
	idx.MustCreateFragmentIfNotExists("d", "f", 0).MustSetBits(200, 20000)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Index = idx.Index

	c := MustNewClient(s.Host())

	// Backup from frame.
	var buf bytes.Buffer
	if err := c.BackupTo(&buf, "d", "f"); err != nil {
		t.Fatal(err)
	}

	// Restore to a different frame.
	if err := c.RestoreFrom(&buf, "x", "y"); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := idx.Fragment("x", "y", 0).Bitmap(100).Bits(); !reflect.DeepEqual(a, []uint64{1, 2, 3, SliceWidth - 1}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := idx.Fragment("x", "y", 1).Bitmap(100).Bits(); !reflect.DeepEqual(a, []uint64{SliceWidth, SliceWidth + 2}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := idx.Fragment("x", "y", 5).Bitmap(100).Bits(); !reflect.DeepEqual(a, []uint64{(5 * SliceWidth) + 1}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := idx.Fragment("x", "y", 0).Bitmap(200).Bits(); !reflect.DeepEqual(a, []uint64{20000}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Client represents a test wrapper for pilosa.Client.
type Client struct {
	*pilosa.Client
}

// MustNewClient returns a new instance of Client. Panic on error.
func MustNewClient(host string) *Client {
	c, err := pilosa.NewClient(host)
	if err != nil {
		panic(err)
	}
	return &Client{Client: c}
}
