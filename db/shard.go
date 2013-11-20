package db

import (
	"fmt"
	"log"
	"github.com/golang/groupcache/lru"
	"pilosa/index"
)

type Request struct {
	action interface{}
}

type IntersectCount struct {
	b1 uint64
	b2 uint64
}

func (g *IntersectCount) Execute(f *Shard) string {
	a := f.Get(g.b1)
	b := f.Get(g.b2)
	result := index.Intersection(a, b)
	c := index.BitCount(result)
	return fmt.Sprintf("%d", c)
}

type UnionCount struct {
	b1 uint64
	b2 uint64
}

func (g *UnionCount) Execute(f *Shard) string {
	b1 := f.Get(g.b1)
	b2 := f.Get(g.b2)
	result := index.Union(b1, b2)
	c := index.BitCount(result)
	return fmt.Sprintf("%d", c)
}

type IdCount struct {
	b1 uint64
}

func (g *IdCount) Execute(f *Shard) string {
	b := f.Get(g.b1)
	//c:=index.BitCount(result)
	c := b.Count()
	return fmt.Sprintf("%d", c)
}

type Stat struct {
	val string
}
type Quit struct {
}

type Shard struct {
	inbound      chan Request
	done         chan bool
	bitmap_cache *lru.Cache
	shard_key    int32
    storage      index.Storage
}

func NewShard(shard_key int32, s index.Storage) *Shard {
	f := new(Shard)
	f.inbound = make(chan Request, 512)
	f.done = make(chan bool)
	f.bitmap_cache = lru.New(10000)
	f.shard_key = shard_key
	f.storage = s
	return f

}

func (f *Shard) Run() {
	for req := range f.inbound {
		switch req.action.(type) {
		default:
			log.Println("Unknown Message")
		case IdCount:
			action := req.action.(IdCount)
			log.Println("IdCount", action.Execute(f))
		case UnionCount:
			action := req.action.(UnionCount)
			log.Println("UnionCount", action.Execute(f))
		case IntersectCount:
			action := req.action.(IntersectCount)
			log.Println("IntersectCount", action.Execute(f))
		case Quit:
			log.Println("Quit", req)
			f.done <- true
		}
	}
}
func (f *Shard) Get(bitmap_id uint64) index.IBitmap {
	bm, ok := f.bitmap_cache.Get(bitmap_id)
	if ok {
		return bm.(*index.Bitmap)
	}
	//need to stick it in the cache
	bm = f.storage.Fetch(bitmap_id, f.shard_key)
	f.bitmap_cache.Add(bitmap_id, bm)
	//return umbel.FetchCass(f.db,bitmap_id,f.shard_key)
	return bm.(*index.Bitmap)
}
func MakeShardKey(property_id int, shard_id int) int32 {
	result := (property_id << 16) | shard_id
	return int32(result)
}

