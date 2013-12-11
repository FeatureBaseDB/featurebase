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

var (
    fragments[] *Fragment
)
type Pilosa interface{
    Union([]uint64) IBitmap
    Intersect([] uint64) IBitmap
  //  SetBit(id uint64, bit_pos int64)bool
}

type RequestJSON struct {
	Request string
    FragmentIndex int
	Args   json.RawMessage
}
type Fragment struct {
	requestChan chan Command
    shardkey int
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

func handler(w http.ResponseWriter, r *http.Request) {
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
            output:=`{"Error":"Invalid FragmentIndex"}`
            if f.FragmentIndex<len(fragments){
                log.Println("Sending Request")
                fc := fragments[f.FragmentIndex]
                fc.requestChan <- request
                output = request.Response()
            }
			fmt.Fprintf(w, output)
		} else {
			fmt.Fprintf(w, "NoOp")
		}
	}
}

func Startup() {
    fragments = append(fragments,&Fragment{make(chan Command),0, NewGeneral("25",0,NewMemoryStorage())}) 
    fragments = append(fragments,&Fragment{make(chan Command),1, NewGeneral("25",1,NewMemoryStorage())})
    fragments = append(fragments,&Fragment{make(chan Command),2, NewGeneral("25",2,NewMemoryStorage())})
    //fragments = append(fragments,&Fragment{make(chan Command),3, &Brand{}})
    for _,v:= range fragments{
        go v.ServeFragment()
    }
}
func Shutdown(){
}

//func Add(db string, slice int, frag_type string,fragment) {
func StartServer(port string, closeChannel chan bool,started chan bool) {
	Startup()
	fmt.Println("Ready")
	http.HandleFunc("/", handler)

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
        Shutdown()
		return
		break
	}
}
