package index

import (
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/cihub/seelog"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/groupcache/lru"
	"github.com/umbel/pilosa/util"
)

// DefaultBackend is the default data storage layer.
const DefaultBackend = "cassandra"

var Backend = DefaultBackend

var LevelDBPath string

type FragmentContainer struct {
	fragments map[util.SUUID]*Fragment
	mutex     *sync.Mutex
}

func lookup(stmt *sql.Stmt, tile_id uint64) int {
	var category int
	err := stmt.QueryRow(tile_id).Scan(&category) // WHERE number = 13
	if err != nil {
		log.Warn(err.Error())
		return 0
	}
	return category

}

func NewFragmentContainer() *FragmentContainer {
	f := new(FragmentContainer)
	f.fragments = make(map[util.SUUID]*Fragment)
	f.mutex = &sync.Mutex{}
	return f
}

type BitmapHandle uint64
type FillArgs struct {
	Frag_id util.SUUID
	Handle  BitmapHandle
	Bitmaps []uint64
}

func init() {
	var vh BitmapHandle
	gob.Register(vh)
	var lp []Pair
	gob.Register(lp)
}

func (self *FragmentContainer) Shutdown() {
	log.Warn("Container Shutdown Started")
	var wg sync.WaitGroup
	wg.Add(len(self.fragments))

	for _, v := range self.fragments {
		v.exit <- &wg
	}
	wg.Wait()
	log.Warn("Container Shutdown Complete")
}

func (self *FragmentContainer) LoadBitmap(frag_id util.SUUID, bitmap_id uint64, compressed_bitmap string, filter uint64) {
	log.Trace("LoadBitmap")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewLoader(bitmap_id, compressed_bitmap, filter)
		fragment.requestChan <- request
		request.Response()
		return
	}
}

func (self *FragmentContainer) GetFragment(frag_id util.SUUID) (*Fragment, bool) {
	log.Trace("index.GetFragment")
	self.mutex.Lock()
	c, v := self.fragments[frag_id]
	self.mutex.Unlock()
	return c, v
}

func (self *FragmentContainer) Stats(frag_id util.SUUID) interface{} {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewStats()
		fragment.requestChan <- request
		return request.Response().answer
	}
	return nil
}
func (self *FragmentContainer) Empty(frag_id util.SUUID) (BitmapHandle, error) {
	log.Trace("index.Empty")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewEmpty()
		fragment.requestChan <- request
		return request.Response().answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle Empty")
}

func (self *FragmentContainer) Intersect(frag_id util.SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	log.Trace("index.Intersect")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewIntersect(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Intersect", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle Intersect")
}
func (self *FragmentContainer) Union(frag_id util.SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	log.Trace("index.Union")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewUnion(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Union", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle Union")
}

func (self *FragmentContainer) Difference(frag_id util.SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	log.Trace("index.Difference")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewDifference(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Difference", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle Diff")
}

func (self *FragmentContainer) Get(frag_id util.SUUID, bitmap_id uint64) (BitmapHandle, error) {
	log.Trace("index.Get")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewGet(bitmap_id)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Get", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle Get")
}

func (self *FragmentContainer) Mask(frag_id util.SUUID, start, end uint64) (BitmapHandle, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewMask(start, end)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Mask", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) Range(frag_id util.SUUID, bitmap_id uint64, start, end time.Time) (BitmapHandle, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewRange(bitmap_id, start, end)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Range", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) TopN(frag_id util.SUUID, bh BitmapHandle, n int, categories []uint64) ([]Pair, error) {
	log.Trace("index.TopN")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewTopN(bh, n, categories)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_TopN", result.exec_time.Nanoseconds())
		return result.answer.([]Pair), nil
	}
	return nil, errors.New(fmt.Sprintf("Fragment not found:%s", util.SUUID_to_Hex(frag_id)))
}

func (self *FragmentContainer) TopNAll(frag_id util.SUUID, n int, categories []uint64) ([]Pair, error) {
	log.Trace("index.TopNAll")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewTopNAll(n, categories)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_TopNAll", result.exec_time.Nanoseconds())
		return result.answer.([]Pair), nil
	}
	return nil, errors.New(fmt.Sprintf("Fragment not found:%s", util.SUUID_to_Hex(frag_id)))
}

func (self *FragmentContainer) TopFillBatch(args []FillArgs) ([]Pair, error) {
	log.Trace("index.TopFillBatch")
	//should probaly make this concurrent but then all hell breaks loose
	results := make(map[uint64]uint64)
	for _, v := range args {
		items, _ := self.TopFillFragment(v)
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
func (self *FragmentContainer) TopFillFragment(arg FillArgs) ([]Pair, error) {
	log.Trace("index.TopFillFragment")
	if fragment, found := self.GetFragment(arg.Frag_id); found {
		request := NewTopFill(arg)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_TopFillFragment", result.exec_time.Nanoseconds())
		return result.answer.([]Pair), nil
	}
	return nil, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) GetList(frag_id util.SUUID, bitmap_id []uint64) ([]BitmapHandle, error) {
	log.Trace("index.GetList")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewGetList(bitmap_id)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_GetList", result.exec_time.Nanoseconds())
		return result.answer.([]BitmapHandle), nil
	}
	return nil, errors.New("Invalid Bitmap Handle GetList")
}

func (self *FragmentContainer) Count(frag_id util.SUUID, bitmap BitmapHandle) (uint64, error) {
	log.Trace("index.Count")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewCount(bitmap)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Count", result.exec_time.Nanoseconds())
		return result.answer.(uint64), nil
	}
	return 0, errors.New("Invalid Bitmap Handle Count")
}

func (self *FragmentContainer) GetBytes(frag_id util.SUUID, bh BitmapHandle) ([]byte, error) {
	log.Trace("index.GetBytes")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewGetBytes(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_GetBytes", result.exec_time.Nanoseconds())
		return result.answer.([]byte), nil
	}
	return nil, errors.New("Invalid Bitmap Handle GetBytes")
}

func (self *FragmentContainer) FromBytes(frag_id util.SUUID, bytes []byte) (BitmapHandle, error) {
	log.Trace("index.FromBytes")
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewFromBytes(bytes)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_FromBytes", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle FromBytes")
}

func (self *FragmentContainer) SetBit(frag_id util.SUUID, bitmap_id uint64, pos uint64, category uint64) (bool, error) {
	log.Trace("SetBit", frag_id, bitmap_id, pos, category)
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewSetBit(bitmap_id, pos, category)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_SetBit", result.exec_time.Nanoseconds())
		util.SendInc("fragmant_container_SetBit")
		return result.answer.(bool), nil
	}
	return false, errors.New("Invalid Bitmap Handle SetBit")
}

func (self *FragmentContainer) ClearBit(frag_id util.SUUID, bitmap_id uint64, pos uint64) (bool, error) {
	log.Trace("ClearBit", frag_id, bitmap_id, pos)
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewClearBit(bitmap_id, pos)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_ClearBit", result.exec_time.Nanoseconds())
		util.SendInc("fragmant_container_ClearBit")
		return result.answer.(bool), nil
	}
	return false, errors.New("Invalid Bitmap Handle ClearBit")
}

func (self *FragmentContainer) Clear(frag_id util.SUUID) (bool, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewClear()
		fragment.requestChan <- request
		return request.Response().answer.(bool), nil
	}
	return false, errors.New("Invalid Fragment ID")
}
func dumpHandlesToLog() {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		log.Warn("Getrlimit:" + err.Error())
	}
	mesg := fmt.Sprintf("%v file descriptors out of a maximum of %v available\n", limit.Cur, limit.Max)
	log.Warn(mesg)
}

func (self *FragmentContainer) AddFragment(db string, frame string, slice int, id util.SUUID) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	_, ok := self.fragments[id]
	if !ok {
		//		dumpHandlesToLog()
		log.Warn("ADD FRAGMENT", frame, db, slice, util.SUUID_to_Hex(id))
		f := NewFragment(id, db, slice, frame)
		loader := make(chan Command)
		self.fragments[id] = f
		go f.ServeFragment(loader)
		go f.Load(loader)
	}

}

type Pilosa interface {
	Get(id uint64) *Bitmap
	SetBit(id uint64, bit_pos uint64, filter uint64) bool
	ClearBit(id uint64, bit_pos uint64) bool
	TopN(b *Bitmap, n int, categories []uint64) []Pair
	TopNAll(n int, categories []uint64) []Pair
	Clear() bool
	Store(bitmap_id uint64, bm *Bitmap, filter uint64)
	Stats() interface{}
	Persist() error
	Load(requestChan chan Command, fragment *Fragment)
	Exists(id uint64) bool
}

type Fragment struct {
	requestChan chan Command
	fragment_id util.SUUID
	impl        Pilosa
	counter     uint64
	slice       int
	cache       *lru.Cache
	mesg_count  uint64
	mesg_time   time.Duration
	exit        chan *sync.WaitGroup
	queue_size  int
}

func NewFragment(frag_id util.SUUID, db string, slice int, frame string) *Fragment {
	var impl Pilosa
	log.Warn(fmt.Sprintf("XXXXXXXXXXXXXXXXXXXXXXXXXXX(%s)", frame))

	storage := NewStorage(Backend, StorageOptions{
		DB:          db,
		Slice:       slice,
		Frame:       frame,
		FragmentID:  frag_id,
		LevelDBPath: LevelDBPath,
	})

	if strings.HasSuffix(frame, ".n") {
		impl = NewBrand(db, frame, slice, storage, 50000, 45000, 100)
	} else {
		impl = NewGeneral(db, frame, slice, storage)
	}

	f := new(Fragment)
	f.requestChan = make(chan Command, 64)
	f.fragment_id = frag_id
	f.cache = lru.New(50000)
	f.impl = impl //NewGeneral(db, slice, NewMemoryStorage())
	f.slice = slice
	f.exit = make(chan *sync.WaitGroup)
	f.queue_size = 0
	return f
}

func (self *Fragment) getBitmap(bitmap BitmapHandle) (*Bitmap, bool) {
	bm, ok := self.cache.Get(bitmap)
	if ok && bm != nil {
		return bm.(*Bitmap), ok
	}
	return NewBitmap(), false //cache fail but return ting em
}

func (self *Fragment) exists(bitmap_id uint64) bool {
	return self.impl.Exists(bitmap_id)
}
func (self *Fragment) TopNAll(n int, categories []uint64) []Pair {
	return self.impl.TopNAll(n, categories)
}

func (self *Fragment) TopN(bitmap BitmapHandle, n int, categories []uint64) []Pair {

	bm, ok := self.cache.Get(bitmap)
	if ok {
		return self.impl.TopN(bm.(*Bitmap), n, categories)
	}
	return nil
}

func (self *Fragment) NewHandle(bitmap_id uint64) BitmapHandle {
	bm := self.impl.Get(bitmap_id)
	return self.AllocHandle(bm)
	//given a bitmap_id return a newly allocated  handle
}
func (self *Fragment) AllocHandle(bm *Bitmap) BitmapHandle {
	handle := self.nextHandle()
	self.cache.Add(handle, bm)
	return handle
}

func (self *Fragment) nextHandle() BitmapHandle {
	millis := uint64(time.Now().UTC().UnixNano())
	id := millis << (64 - 41)
	id |= uint64(self.slice) << (64 - 41 - 13)
	id |= self.counter % 1024
	self.counter += 1
	return BitmapHandle(id)
}

func (self *Fragment) union(bitmaps []BitmapHandle) BitmapHandle {
	result := NewBitmap()
	for i, id := range bitmaps {
		bm, _ := self.getBitmap(id)
		if i == 0 {
			result = bm
		} else {
			result = result.Union(bm)
		}
	}
	return self.AllocHandle(result)
}
func (self *Fragment) build_time_range_bitmap(bitmap_id uint64, start, end time.Time) BitmapHandle {
	result := NewBitmap()
	for i, bid := range GetRange(start, end, bitmap_id) {
		bm := self.impl.Get(bid)
		if i == 0 {
			result = bm
		} else {
			result = result.Union(bm)
		}
	}
	return self.AllocHandle(result)
}

func (self *Fragment) intersect(bitmaps []BitmapHandle) BitmapHandle {
	var result *Bitmap
	for i, id := range bitmaps {
		bm, _ := self.getBitmap(id)
		if i == 0 {
			result = bm.Clone()
		} else {
			result = result.Intersection(bm)
		}
	}
	return self.AllocHandle(result)
}

func (self *Fragment) difference(bitmaps []BitmapHandle) BitmapHandle {
	result := NewBitmap()
	for i, id := range bitmaps {
		bm, _ := self.getBitmap(id)
		if i == 0 {
			result = bm
		} else {
			result = result.Difference(bm)
		}
	}
	return self.AllocHandle(result)
}

func (self *Fragment) Persist() {
	err := self.impl.Persist()
	if err != nil {
		log.Warn("Error saving:", err)
	}
}
func (self *Fragment) Load(loadChan chan Command) {
	self.impl.Load(loadChan, self)
}

func (self *Fragment) processCommand(req Command) {
	self.mesg_count++
	start := time.Now()
	answer := req.Execute(self)
	delta := time.Since(start)
	self.mesg_count += 1
	self.mesg_time += delta
	req.ResponseChannel() <- Result{answer, delta}
}
func (self *Fragment) ServeFragment(loadChan chan Command) {
	for {
		select {
		case req := <-self.requestChan:
			self.processCommand(req)
		default:
			select {
			case req := <-self.requestChan:
				self.processCommand(req)
			case req := <-loadChan:
				self.processCommand(req)
			case wg := <-self.exit:
				log.Warn("Fragment Shutdown")
				self.Persist()
				wg.Done()
			}
		}
	}
}
