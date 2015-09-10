package pilosa

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"syscall"
	"time"

	log "github.com/cihub/seelog"
	// "github.com/umbel/pilosa/statsd"
)

// DefaultBackend is the default data storage layer.
const DefaultBackend = "cassandra"

var Backend = DefaultBackend

var LevelDBPath string

var (
	ErrFragmentNotFound = errors.New("fragment not found")
)

func init() {
	gob.Register(BitmapHandle(0))
	gob.Register([]Pair{})
}

type FragmentContainer struct {
	mu        sync.Mutex
	fragments map[SUUID]*Fragment
}

func NewFragmentContainer() *FragmentContainer {
	return &FragmentContainer{
		fragments: make(map[SUUID]*Fragment),
	}
}

type BitmapHandle uint64

type FillArgs struct {
	FragmentID SUUID
	Handle     BitmapHandle
	Bitmaps    []uint64
}

func (fc *FragmentContainer) Fragment(fragmentID SUUID) (*Fragment, bool) {
	fc.mu.Lock()
	f, ok := fc.fragments[fragmentID]
	fc.mu.Unlock()
	return f, ok
}

func (fc *FragmentContainer) Get(fragmentID SUUID, bitmapID uint64) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}
	return f.NewHandle(bitmapID), nil
}

func (fc *FragmentContainer) LoadBitmap(fragmentID SUUID, bitmapID uint64, data string, filter uint64) error {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return ErrFragmentNotFound
	}

	// Decode from base64 encoding.
	buf, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return err
	}

	// Decompress data.
	reader, err := gzip.NewReader(bytes.NewReader(buf))
	if err != nil {
		return err
	}
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	// Build bitmap from data.
	bm := NewBitmap()
	bm.FromBytes(b)

	// Write bitmap to the underlying store.
	return f.impl.Store(bitmapID, bm, filter)
}

func (fc *FragmentContainer) Stats(fragmentID SUUID) interface{} {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return nil
	}
	return f.impl.Stats()
}

func (fc *FragmentContainer) Empty(fragmentID SUUID) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}
	return f.AllocHandle(NewBitmap()), nil
}

func (fc *FragmentContainer) Intersect(fragmentID SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}
	return f.Intersect(bh), nil
}

func (fc *FragmentContainer) Union(fragmentID SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}
	return f.Union(bh), nil
}

func (fc *FragmentContainer) Difference(fragmentID SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}
	return f.Difference(bh), nil
}

func (fc *FragmentContainer) Mask(fragmentID SUUID, start, end uint64) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}

	bm := NewBitmap()
	for i := start; i < end; i++ {
		bm.SetBit(i)
	}
	return f.AllocHandle(bm), nil
}

func (fc *FragmentContainer) Range(fragmentID SUUID, bitmapID uint64, start, end time.Time) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}
	return f.build_time_range_bitmap(bitmapID, start, end), nil
}

func (fc *FragmentContainer) TopN(fragmentID SUUID, bh BitmapHandle, n int, categories []uint64) ([]Pair, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return nil, ErrFragmentNotFound
	}
	return f.TopN(bh, n, categories), nil
}

func (fc *FragmentContainer) TopNAll(fragmentID SUUID, n int, categories []uint64) ([]Pair, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return nil, ErrFragmentNotFound
	}
	return f.TopNAll(n, categories), nil
}

func (fc *FragmentContainer) TopFillBatch(args []FillArgs) ([]Pair, error) {
	results := make(map[uint64]uint64)
	for _, v := range args {
		items, _ := fc.TopFillFragment(v)
		if len(args) == 1 {
			return items, nil
		}
		for _, pair := range items {
			results[pair.Key] += pair.Count
		}
	}
	ret_val := make([]Pair, len(results))
	for k, v := range results {
		if v > 0 { //don't include 0 size items
			ret_val = append(ret_val, Pair{k, v})
		}
	}
	return ret_val, nil

}
func (fc *FragmentContainer) TopFillFragment(args FillArgs) ([]Pair, error) {
	f, ok := fc.Fragment(args.FragmentID)
	if !ok {
		return nil, ErrFragmentNotFound
	}

	result := make([]Pair, 0)
	for _, v := range args.Bitmaps {
		if !f.exists(v) {
			continue
		}

		a := f.NewHandle(v)
		if args.Handle == 0 {
			// Return just the count
			if bm, ok := f.Bitmap(a); ok && bm.Count() > 0 {
				result = append(result, Pair{v, bm.Count()})
			}
			continue
		}

		res := f.Intersect([]BitmapHandle{args.Handle, a})
		if bm, ok := f.Bitmap(res); ok {
			bc := bm.BitCount()
			if bc > 0 {
				result = append(result, Pair{v, bc})
			}
		}
	}
	return result, nil
}

func (fc *FragmentContainer) GetList(fragmentID SUUID, bitmapIDs []uint64) ([]BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return nil, ErrFragmentNotFound
	}

	a := make([]BitmapHandle, len(bitmapIDs))
	for i, v := range bitmapIDs {
		a[i] = f.NewHandle(v)
	}
	return a, nil
}

func (fc *FragmentContainer) Count(fragmentID SUUID, bh BitmapHandle) (uint64, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}

	bm, ok := f.Bitmap(bh)
	if ok == false {
		return 0, nil
	}
	return bm.BitCount(), nil
}

func (fc *FragmentContainer) GetBytes(fragmentID SUUID, bh BitmapHandle) ([]byte, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return nil, ErrFragmentNotFound
	}

	bm, ok := f.Bitmap(bh)
	if !ok {
		bm = NewBitmap()
		log.Warn("cache miss")
	}

	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(bm.ToBytes()); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (fc *FragmentContainer) FromBytes(fragmentID SUUID, data []byte) (BitmapHandle, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return 0, ErrFragmentNotFound
	}

	r, _ := gzip.NewReader(bytes.NewReader(data))
	b, _ := ioutil.ReadAll(r)

	bm := NewBitmap()
	bm.FromBytes(b)
	return f.AllocHandle(bm), nil
}

func (fc *FragmentContainer) SetBit(fragmentID SUUID, bitmapID uint64, pos uint64, category uint64) (bool, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return false, ErrFragmentNotFound
	}
	return f.impl.SetBit(bitmapID, pos, category), nil
}

func (fc *FragmentContainer) ClearBit(fragmentID SUUID, bitmapID uint64, pos uint64) (bool, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return false, ErrFragmentNotFound
	}
	return f.impl.ClearBit(bitmapID, pos), nil
}

func (fc *FragmentContainer) Clear(fragmentID SUUID) (bool, error) {
	f, ok := fc.Fragment(fragmentID)
	if !ok {
		return false, ErrFragmentNotFound
	}
	return f.impl.Clear(), nil
}

func (fc *FragmentContainer) AddFragment(db string, frame string, slice int, id SUUID) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	_, ok := fc.fragments[id]
	if ok {
		return
	}

	log.Warn("ADD FRAGMENT", frame, db, slice, id.String())
	f := NewFragment(id, db, slice, frame)
	fc.fragments[id] = f
	// go f.Load(loader)
}

func dumpHandlesToLog() {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		log.Warn("Getrlimit:" + err.Error())
	}
	mesg := fmt.Sprintf("%v file descriptors out of a maximum of %v available\n", limit.Cur, limit.Max)
	log.Warn(mesg)
}
