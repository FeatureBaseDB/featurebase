package pilosa_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestAPI_QueryFailsWhenClusterNotReady(t *testing.T) {
	api := pilosa.NewAPI()
	api.Cluster = pilosa.NewCluster()
	_, err := api.Query(nil, nil)
	if err == nil {
		t.Fatalf("Should have failed.")
	}
}

func TestAPI_CreateIndexWhenClusterNotReady(t *testing.T) {
	api := pilosa.NewAPI()
	api.Cluster = pilosa.NewCluster()
	_, err := api.CreateIndex(nil, "", pilosa.IndexOptions{})
	if err == nil {
		t.Fatalf("Should have failed.")
	}
}

func TestAPI_ExportCSV(t *testing.T) {
	api := pilosa.NewAPI()
	api.Cluster = pilosa.NewCluster()
	err := api.ExportCSV(nil, "", "", "", 0, nil)
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	uri1 := test.NewURIFromHostPort("localhost", 10101)
	uri2 := test.NewURIFromHostPort("localhost", 10102)
	api.Cluster.Node = &pilosa.Node{ID: "id1", URI: uri1}
	api.Cluster.Nodes = []*pilosa.Node{api.Cluster.Node, &pilosa.Node{ID: "id2", URI: uri2}}
	api.Cluster.SetState(pilosa.ClusterStateNormal)
	api.Cluster.Hasher = &FakeHasher{predefinedHash: 1}
	err = api.ExportCSV(nil, "i1", "f1", "standard", 0, nil)
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	api.Holder = pilosa.NewHolder()
	api.Cluster.Hasher = &FakeHasher{predefinedHash: 0}
	err = api.ExportCSV(nil, "i1", "f1", "standard", 0, nil)
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}
	hldr := test.MustOpenHolder()
	hldr.Path = path
	defer hldr.Close()
	f1 := hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0)
	if _, err := f1.SetBit(100, 200); err != nil {
		t.Fatal(err)
	}
	api.Holder = hldr.Holder
	buf := bytes.NewBuffer([]byte{})
	err = api.ExportCSV(nil, "i1", "f1", pilosa.ViewStandard, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	target := "100,200\n"
	if buf.String() != target {
		t.Fatalf("%v != %v", buf.Bytes(), []byte(target))
	}
}

func TestAPI_SetCoordinator(t *testing.T) {
	api := pilosa.NewAPI()
	api.Cluster = pilosa.NewCluster()
	_, _, err := api.SetCoordinator(nil, "id1")
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	api.Cluster = test.NewCluster(1)

	_, _, err = api.SetCoordinator(nil, "nonexistent-id")
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	c := test.NewTestCluster(1)
	api.Cluster = c.Clusters[0]
	api.Cluster.SetState(pilosa.ClusterStateNormal)
	_, _, err = api.SetCoordinator(nil, "node0")
	if err != nil {
		t.Fatal(err)
	}

	err = c.AddNode(false)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = api.SetCoordinator(nil, "node1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestAPI_Index(t *testing.T) {
	api := pilosa.NewAPI()
	api.Cluster = pilosa.NewCluster()
	_, err := api.Index(nil, "i1")
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	c := test.NewTestCluster(1)
	api.Holder = pilosa.NewHolder()
	api.Cluster = c.Clusters[0]
	api.Cluster.SetState(pilosa.ClusterStateNormal)
	_, err = api.Index(nil, "i1")
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}
	hldr := test.MustOpenHolder()
	api.Holder = hldr.Holder
	hldr.Path = path
	defer hldr.Close()
	hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{})
	_, err = api.Index(nil, "i1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestAPI_RestoreFrame(t *testing.T) {
	uri := test.NewURIFromHostPort("localhost", 10101)

	api := pilosa.NewAPI()
	api.Cluster = pilosa.NewCluster()
	err := api.RestoreFrame(nil, "i1", "f1", &uri)
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	ctx := context.WithValue(context.Background(), "uri", uri)
	c := test.NewTestCluster(1)
	api.Holder = pilosa.NewHolder()
	api.Cluster = c.Clusters[0]
	api.Cluster.SetState(pilosa.ClusterStateNormal)
	api.RemoteClient = &http.Client{}
	err = api.RestoreFrame(ctx, "i1", "f1", &uri)
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	s := test.NewServer()
	defer s.Close()
	uri = s.HostURI()

	s.Handler.API.Holder = pilosa.NewHolder()
	ctx = context.WithValue(context.Background(), "uri", uri)
	c = test.NewTestCluster(1)
	api.Holder = pilosa.NewHolder()
	api.Cluster = c.Clusters[0]
	api.Cluster.SetState(pilosa.ClusterStateNormal)
	api.RemoteClient = &http.Client{}
	err = api.RestoreFrame(ctx, "i1", "f1", &uri)
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	/*
		path, err := ioutil.TempDir("", "pilosa-")
		if err != nil {
			panic(err)
		}
		hldr := test.MustOpenHolder()
		hldr.Holder.Path = path
		defer hldr.Close()
		hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0)
		s.Handler.API.Holder = hldr.Holder
		err = api.RestoreFrame(ctx, "i1", "f1", &uri)
		if err != nil {
			t.Fatal(err)
		}
	*/
}

func TestAPI_RemoveNode(t *testing.T) {
	api := pilosa.NewAPI()
	api.Cluster = pilosa.NewCluster()
	_, err := api.RemoveNode("id1")
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	c := test.NewTestCluster(1)
	api.Holder = pilosa.NewHolder()
	api.Cluster = c.Clusters[0]
	api.Cluster.SetState(pilosa.ClusterStateNormal)
	_, err = api.RemoveNode("nonexistent-id")
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	_, err = api.RemoveNode("node0")
	if err == nil {
		t.Fatalf("Should have failed.")
	}

	err = c.AddNode(false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.RemoveNode("node1")
	if err != nil {
		t.Fatal(err)
	}
}

type FakeHasher struct {
	predefinedHash int
}

func (b *FakeHasher) Hash(key uint64, n int) int {
	return b.predefinedHash
}
