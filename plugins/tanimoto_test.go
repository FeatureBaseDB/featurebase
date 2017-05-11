package plugins

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
	"io/ioutil"
	"strings"
	"testing"
)

type Fragment struct {
	*pilosa.Fragment
}

func (f *Fragment) MustSetBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.SetBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}

type Holder struct {
	*pilosa.Holder
	LogOutput bytes.Buffer
}

func NewHolder() *Holder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &Holder{Holder: pilosa.NewHolder()}
	h.Path = path
	h.Holder.LogOutput = &h.LogOutput

	return h
}

func MustOpenHolder() *Holder {
	h := NewHolder()
	if err := h.Open(); err != nil {
		panic(err)
	}
	return h
}

type Index struct {
	*pilosa.Index
}

type Frame struct {
	*pilosa.Frame
}

func (h *Holder) MustCreateIndexIfNotExists(index string, opt pilosa.IndexOptions) *Index {
	idx, err := h.Holder.CreateIndexIfNotExists(index, opt)
	if err != nil {
		panic(err)
	}
	return &Index{Index: idx}
}

func (h *Holder) MustCreateFrameIfNotExists(index, frame string) *pilosa.Frame {
	f, err := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{}).CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
	if err != nil {
		panic(err)
	}
	return f
}

func (h *Holder) MustCreateFragmentIfNotExists(index, frame, view string, slice uint64) *Fragment {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
	if err != nil {
		panic(err)
	}
	v, err := f.CreateViewIfNotExists(view)
	if err != nil {
		panic(err)
	}
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		panic(err)
	}
	return &Fragment{Fragment: frag}
}

type Executor struct {
	*pilosa.Executor
}

func NewExecutor(holder *pilosa.Holder, cluster *pilosa.Cluster) *Executor {
	e := &Executor{Executor: pilosa.NewExecutor()}
	e.Holder = holder
	e.Cluster = cluster
	e.Host = cluster.Nodes[0].Host
	return e
}

func NewCluster(n int) *pilosa.Cluster {
	c := pilosa.NewCluster()
	c.ReplicaN = 1

	for i := 0; i < n; i++ {
		c.Nodes = append(c.Nodes, &pilosa.Node{
			Host: fmt.Sprintf("host%d", i),
		})
	}

	return c
}

func MustParse(s string) *pql.Query {
	q, err := pql.NewParser(strings.NewReader(s)).Parse()
	if err != nil {
		panic(err)
	}
	return q
}

func TestExecutor_Execute_Tanimoto(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	frag := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	frag.Fragment.CacheType = pilosa.CacheTypeRanked
	frag.MustSetBits(100, 1, 3, 2, 200)
	frag.MustSetBits(101, 1, 3)
	frag.MustSetBits(102, 1, 2, 10, 12)
	frag.RecalculateCache()

	e := NewExecutor(hldr.Holder, NewCluster(1))

	res, err := e.Execute(context.Background(), "i", MustParse(`Tanimoto(Bitmap(rowID=102, frame=f), frame=f, threshold=50)`), nil, nil)

	if err != nil {
		t.Fatal(err)
	} else if res[0].([]pilosa.Pair)[0].ID != 102 {
		t.Fatalf("unexpected result: %v", res[0])
	}

}

func TestExecutor_Execute_ZeroTanimoto(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	frag := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	frag.Fragment.CacheType = pilosa.CacheTypeRanked
	frag.MustSetBits(100, 1, 3, 2, 200)
	frag.MustSetBits(101, 1, 3)
	frag.MustSetBits(102, 1, 2, 10, 12)
	frag.RecalculateCache()

	e := NewExecutor(hldr.Holder, NewCluster(1))

	res, err := e.Execute(context.Background(), "i", MustParse(`Tanimoto(Bitmap(rowID=102, frame=f), frame=f, threshold=0)`), nil, nil)
	if err != nil {
		t.Fatal(err)
	} else if len(res[0].([]pilosa.Pair)) > 0 {
		t.Fatalf("unexpected result: %v", res[0])
	}

}

func TestExecutor_Execute_InvalidThreshold(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	frag := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	frag.Fragment.CacheType = pilosa.CacheTypeRanked
	frag.MustSetBits(100, 1, 3, 2, 200)
	frag.MustSetBits(101, 1, 3)
	frag.MustSetBits(102, 1, 2, 10, 12)
	frag.RecalculateCache()

	e := NewExecutor(hldr.Holder, NewCluster(1))

	_, err := e.Execute(context.Background(), "i", MustParse(`Tanimoto(Bitmap(rowID=102, frame=f), frame=f, threshold=101)`), nil, nil)
	if err == nil {
		t.Fatal(err)
	}
}
