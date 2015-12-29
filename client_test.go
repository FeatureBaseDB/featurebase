package pilosa_test

import (
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
