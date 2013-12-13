package index

import (
	"errors"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/nu7hatch/gouuid"
)

type FragmentContainer struct {
	fragments map[*uuid.UUID]*Fragment
}

func NewFragmentContainer() *FragmentContainer {
	return &FragmentContainer{make(map[*uuid.UUID]*Fragment)}
}

type BitmapHandle uint64

func (a *FragmentContainer) GetFragment(frag_guid *uuid.UUID) (*Fragment, bool) {
	//lock
	c, v := a.fragments[frag_guid]
	return c, v
}

func (a *FragmentContainer) Intersect(frag_guid *uuid.UUID, bh []BitmapHandle) (BitmapHandle, error) {
	if fragment, found := a.GetFragment(frag_guid); found {
		request := NewIntersect(bh)
		fragment.requestChan <- request
		return request.GetResponder().Response().answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}
func (a *FragmentContainer) Union(frag_guid *uuid.UUID, bh []BitmapHandle) (BitmapHandle, error) {
	if fragment, found := a.GetFragment(frag_guid); found {
		request := NewUnion(bh)
		fragment.requestChan <- request
		return request.GetResponder().Response().answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (a *FragmentContainer) Get(frag_guid *uuid.UUID, bitmap_id uint64) (BitmapHandle, error) {
	if fragment, found := a.GetFragment(frag_guid); found {
		request := NewGet(bitmap_id)
		fragment.requestChan <- request
		return request.GetResponder().Response().answer.(BitmapHandle), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}
func (a *FragmentContainer) Count(frag_guid *uuid.UUID, bitmap BitmapHandle) (uint64, error) {
	if fragment, found := a.GetFragment(frag_guid); found {
		request := NewCount(bitmap)
		fragment.requestChan <- request
		return request.GetResponder().Response().answer.(uint64), nil
	}
	return 0, errors.New("Invalid Bitmap Handle")
}

func (a *FragmentContainer) SetBit(frag_guid *uuid.UUID, bitmap BitmapHandle, pos uint64) (bool, error) {
	if fragment, found := a.GetFragment(frag_guid); found {
		request := NewSetBit(bitmap, pos)
		fragment.requestChan <- request
		return request.GetResponder().Response().answer.(bool), nil
	}
	return false, errors.New("Invalid Bitmap Handle")
}

func (a *FragmentContainer) AddFragment(frame string, db string, slice int, guid *uuid.UUID) {
	f := NewFragment(guid, db, slice, frame)
	a.fragments[guid] = f
	go f.ServeFragment()
}

type Pilosa interface {
	Get(id uint64) IBitmap
}

type Fragment struct {
	requestChan   chan Command
	fragment_uuid *uuid.UUID
	impl          Pilosa
	counter       uint64
	slice         int
	cache         *lru.Cache
}

func NewFragment(guid *uuid.UUID, db string, slice int, frame string) *Fragment {
	f := new(Fragment)
	f.requestChan = make(chan Command, 64)
	f.fragment_uuid = guid
	f.cache = lru.New(10000)
	f.impl = NewGeneral(db, slice, NewMemoryStorage())
	f.slice = slice
	return f
}

func (f *Fragment) getBitmap(bitmap BitmapHandle) (IBitmap, bool) {
	bm, ok := f.cache.Get(bitmap)
	return bm.(IBitmap), ok
}

func (f *Fragment) NewHandle(bitmap_id uint64) BitmapHandle {
	bm := f.impl.Get(bitmap_id)
	return f.AllocHandle(bm)
	//given a bitmap_id return a newly allocated  handle
}
func (f *Fragment) AllocHandle(bm IBitmap) BitmapHandle {
	handle := f.nextHandle()
	f.cache.Add(handle, bm)
	return handle
}

func (f *Fragment) nextHandle() BitmapHandle {
	millis := uint64(time.Now().UTC().UnixNano())
	id := millis << (64 - 41)
	id |= uint64(f.slice) << (64 - 41 - 13)
	id |= f.counter % 1024
	f.counter += 1
	return BitmapHandle(id)
}

func (f *Fragment) union(bitmaps []BitmapHandle) BitmapHandle {
	result := NewBitmap()
	for i, id := range bitmaps {
		bm, _ := f.getBitmap(id)
		if i == 0 {
			result = bm
		} else {
			result = Union(result, bm)
		}
	}
	return f.AllocHandle(result)
}
func (f *Fragment) intersect(bitmaps []BitmapHandle) BitmapHandle {
	var result IBitmap
	for i, id := range bitmaps {
		bm, _ := f.getBitmap(id)
		if i == 0 {
			result = Clone(bm)
		} else {
			result = Intersection(result, bm)
		}
	}
	return f.AllocHandle(result)
}

func (f *Fragment) ServeFragment() {
	for {
		req := <-f.requestChan
		start := time.Now()
		responder := req.GetResponder()
		answer := req.Execute(f)
		delta := time.Since(start)
		/*
			var buffer bytes.Buffer
			buffer.WriteString(`{ "results":`)
			buffer.WriteString(answer)
			buffer.WriteString(fmt.Sprintf(`,"query type": "%s"`, responder.QueryType()))
			buffer.WriteString(fmt.Sprintf(`, "elapsed": "%s"}`, delta))
		*/
		responder.ResponseChannel() <- Result{answer, delta}
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
