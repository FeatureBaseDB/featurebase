package index

import (
	"database/sql"

	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"pilosa/config"
	"pilosa/util"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/groupcache/lru"
)

type FragmentContainer struct {
	fragments map[util.SUUID]*Fragment
}

func lookup(stmt *sql.Stmt, tile_id uint64) int {
	var category int
	err := stmt.QueryRow(tile_id).Scan(&category) // WHERE number = 13
	if err != nil {
		log.Println(err.Error())
		return 0
	}
	return category

}

func NewFragmentContainer() *FragmentContainer {
	f := new(FragmentContainer)
	f.fragments = make(map[util.SUUID]*Fragment)
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
	log.Println("Container Shutdown Started")
	var wg sync.WaitGroup
	wg.Add(len(self.fragments))

	for _, v := range self.fragments {
		v.exit <- &wg
	}
	wg.Wait()
	log.Println("Container Shutdown Complete")
}

func (self *FragmentContainer) LoadBitmap(frag_id util.SUUID, bitmap_id uint64, compressed_bitmap string, filter uint64) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewLoader(bitmap_id, compressed_bitmap, filter)
		fragment.requestChan <- request
		request.Response()
		return
	}
}

func (self *FragmentContainer) GetFragment(frag_id util.SUUID) (*Fragment, bool) {
	//lock
	c, v := self.fragments[frag_id]
	//log.Println(self.fragments)
	//log.Println(c)
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
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewEmpty()
		fragment.requestChan <- request
		return request.Response().answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) Intersect(frag_id util.SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewIntersect(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Intersect", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}
func (self *FragmentContainer) Union(frag_id util.SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewUnion(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Union", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) Difference(frag_id util.SUUID, bh []BitmapHandle) (BitmapHandle, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewDifference(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Difference", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) Get(frag_id util.SUUID, bitmap_id uint64) (BitmapHandle, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewGet(bitmap_id)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Get", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
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
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewTopN(bh, n, categories)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_TopN", result.exec_time.Nanoseconds())
		return result.answer.([]Pair), nil
	}
	return nil, errors.New(fmt.Sprintf("Fragment not found:%s", util.SUUID_to_Hex(frag_id)))
}
func (self *FragmentContainer) TopFillBatch(args []FillArgs) ([]Pair, error) {
	//should probaly make this concurrent but then all hell breaks lose
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
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewGetList(bitmap_id)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_GetList", result.exec_time.Nanoseconds())
		return result.answer.([]BitmapHandle), nil
	}
	return nil, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) Count(frag_id util.SUUID, bitmap BitmapHandle) (uint64, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewCount(bitmap)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_Count", result.exec_time.Nanoseconds())
		return result.answer.(uint64), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) GetBytes(frag_id util.SUUID, bh BitmapHandle) ([]byte, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewGetBytes(bh)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_GetBytes", result.exec_time.Nanoseconds())
		return result.answer.([]byte), nil
	}
	return nil, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) FromBytes(frag_id util.SUUID, bytes []byte) (BitmapHandle, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewFromBytes(bytes)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_FromBytes", result.exec_time.Nanoseconds())
		return result.answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) SetBit(frag_id util.SUUID, bitmap_id uint64, pos uint64, category uint64) (bool, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewSetBit(bitmap_id, pos, category)
		fragment.requestChan <- request
		result := request.Response()
		util.SendTimer("fragmant_container_SetBit", result.exec_time.Nanoseconds())
		return result.answer.(bool), nil
	}
	return false, errors.New("Invalid Bitmap Handle")
}

func (self *FragmentContainer) Clear(frag_id util.SUUID) (bool, error) {
	if fragment, found := self.GetFragment(frag_id); found {
		request := NewClear()
		fragment.requestChan <- request
		return request.Response().answer.(bool), nil
	}
	return false, errors.New("Invalid Fragment ID")
}

func (self *FragmentContainer) AddFragment(db string, frame string, slice int, id util.SUUID) {
	_, ok := self.fragments[id]
	if !ok {
		log.Println("ADD FRAGMENT", frame, db, slice, util.SUUID_to_Hex(id))
		f := NewFragment(id, db, slice, frame)
		self.fragments[id] = f
		go f.ServeFragment()
		go f.Load()
	}

}

type Pilosa interface {
	Get(id uint64) IBitmap
	SetBit(id uint64, bit_pos uint64, filter uint64) bool
	TopN(b IBitmap, n int, categories []uint64) []Pair
	Clear() bool
	Store(bitmap_id uint64, bm IBitmap, filter uint64)
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
}

func getStorage(db string, slice int, frame string, fid util.SUUID) Storage {
	storage_method := config.GetString("storage_backend")

	switch storage_method {
	default:
		return NewMemoryStorage()
	case "leveldb":
		base_path := config.GetString("level_db_path")
		full_dir := fmt.Sprintf("%s/%s/%d/%s/%s", base_path, db, slice, frame, util.SUUID_to_Hex(fid))
		return NewLevelDBStorage(full_dir)
	case "cassandra":
		return NewCassStorage()
	}
}

func NewFragment(frag_id util.SUUID, db string, slice int, frame string) *Fragment {
	var impl Pilosa
	log.Println(fmt.Sprintf("XXXXXXXXXXXXXXXXXXXXXXXXXXX(%s)", frame))
	if strings.HasSuffix(frame, ".n") {
		impl = NewBrand(db, frame, slice, getStorage(db, slice, frame, frag_id), 50000, 45000, 100)
	} else {
		impl = NewGeneral(db, frame, slice, getStorage(db, slice, frame, frag_id))
	}

	f := new(Fragment)
	f.requestChan = make(chan Command, 64)
	f.fragment_id = frag_id
	f.cache = lru.New(10000)
	f.impl = impl //NewGeneral(db, slice, NewMemoryStorage())
	f.slice = slice
	f.exit = make(chan *sync.WaitGroup)
	return f
}

func (self *Fragment) getBitmap(bitmap BitmapHandle) (IBitmap, bool) {
	bm, ok := self.cache.Get(bitmap)
	return bm.(IBitmap), ok
}

func (self *Fragment) exists(bitmap_id uint64) bool {
	return self.impl.Exists(bitmap_id)
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
func (self *Fragment) AllocHandle(bm IBitmap) BitmapHandle {
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
			result = Union(result, bm)
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
			result = Union(result, bm)
		}
	}
	return self.AllocHandle(result)
}

func (self *Fragment) intersect(bitmaps []BitmapHandle) BitmapHandle {
	var result IBitmap
	for i, id := range bitmaps {
		bm, _ := self.getBitmap(id)
		if i == 0 {
			result = Clone(bm)
		} else {
			result = Intersection(result, bm)
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
			result = Difference(result, bm)
		}
	}
	return self.AllocHandle(result)
}

func (self *Fragment) Persist() {
	err := self.impl.Persist()
	if err != nil {
		log.Println("Error saving:", err)
	}
}
func (self *Fragment) Load() {
	self.impl.Load(self.requestChan, self)
}

func (self *Fragment) ServeFragment() {
	for {
		select {
		case req := <-self.requestChan:
			self.mesg_count++
			start := time.Now()
			answer := req.Execute(self)
			delta := time.Since(start)
			self.mesg_count += 1
			self.mesg_time += delta
			req.ResponseChannel() <- Result{answer, delta}

		case wg := <-self.exit:
			log.Println("Fragment Shutdown")
			self.Persist()
			wg.Done()
		}
	}
}

/*
type RequestJSON struct {
	Request  string
	Fragment string
	Args     json.RawMessage
}
func (a *FragmentContainer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler(w, r, a.fragments)
}

func (a *FragmentContainer) RunServer(porti int, closeChannel chan bool, started chan bool) {
	http.Handle("/", a)
	port := fmt.Sprintf(":%d", porti)

	s := &http.Server{
		Addr:           port,
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	l, e := net.Listen("tcp", port)
	if e != nil {
		log.Panicf(e.Error())
	}
	go s.Serve(l)
	started <- true
	select {
	case <-closeChannel:
		log.Printf("Server thread exit")
		l.Close()
		// Shutdown()
		return
		break
	}
}

func handler(w http.ResponseWriter, r *http.Request, fragments map[string]*Fragment) {
	if r.Method == "POST" {
		var f RequestJSON

		bin, _ := ioutil.ReadAll(r.Body)
		err := json.Unmarshal(bin, &f)

		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, fmt.Sprintf(`{ "error":"%s"}`, err))

		}
		decoder := json.NewDecoder(bytes.NewReader(f.Args))
		request := BuildCommandFactory(&f, decoder)
		w.Header().Set("Content-Type", "application/json")
		if request != nil {
			output := `{"Error":"Invalid Fragment"}`
			fc, found := fragments[f.Fragment] //f.FragmentIndex<len(fragments){
			if found {
				//   fc := fragments[f.FragmentGuid]
				fc.requestChan <- request
				output = request.GetResponder().Response()
			}
			fmt.Fprintf(w, output)
		} else {
			fmt.Fprintf(w, "NoOp")
		}
	}
}
*/
