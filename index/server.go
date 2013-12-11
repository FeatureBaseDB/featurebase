package index

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	//"sort"
	"time"
    "log"
)

type FragmentContainer struct {
    fragments map[string] *Fragment
}
func NewFragmentContainer() *FragmentContainer{
    return &FragmentContainer{make( map[string]*Fragment)}
}
 
func (a *FragmentContainer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
     handler(w , r,a.fragments) 
}

func (a *FragmentContainer) AddFragment(frame string, db string, slice int, frag_guid string) {
    f :=&Fragment{make(chan Command),frag_guid, NewGeneral(db,slice,NewMemoryStorage())}
    a.fragments[frag_guid] = f
    go f.ServeFragment() 
}

func (a *FragmentContainer) RunServer(porti int, closeChannel chan bool,started chan bool) {
	http.Handle("/", a)
    port := fmt.Sprintf(":%d",porti)

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
    started<- true
	select {
	case <-closeChannel:
		log.Printf("Server thread exit")
		l.Close()
       // Shutdown()
		return
		break
	}
}

type Pilosa interface{
    Union([]uint64) IBitmap
    Intersect([] uint64) IBitmap
  //  SetBit(id uint64, bit_pos int64)bool
}

type RequestJSON struct {
	Request string
    Fragment string
	Args   json.RawMessage
}
type Fragment struct {
	requestChan chan Command
    FragmentGuid string 
    impl Pilosa
}

func (f *Fragment) ServeFragment() {
	for {
		req := <-f.requestChan
		start := time.Now()
		answer := `""`
		answer = req.Execute(f)
		delta := time.Since(start)
		var buffer bytes.Buffer
		buffer.WriteString(`{ "results":`)
		buffer.WriteString(answer)
		buffer.WriteString(fmt.Sprintf(`,"query type": "%s"`, req.QueryType()))
		buffer.WriteString(fmt.Sprintf(`, "elapsed": "%s"}`, delta))
		req.ResponseChannel() <- buffer.String()
	}
}

func handler(w http.ResponseWriter, r *http.Request,fragments map[string]*Fragment) {
    log.Println("GOT MESSAGE")
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
            output:=`{"Error":"Invalid Fragment"}`
            fc,found :=  fragments[f.Fragment] //f.FragmentIndex<len(fragments){
            if found{
                log.Println("Sending Request")
             //   fc := fragments[f.FragmentGuid]
                fc.requestChan <- request
                output = request.Response()
            }
			fmt.Fprintf(w, output)
		} else {
			fmt.Fprintf(w, "NoOp")
		}
	}
}

