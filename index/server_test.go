package index

import (
    "testing"
    . "github.com/smartystreets/goconvey/convey"
    "net/http"
//	"encoding/json"
	"io/ioutil"
    "time"
    "log"
    "strings"
    )
    
    func simple(){
        msg:=`
        {
            "Request": "UnionCount",
            "Fragment": 0,
            "Args": {  
                "Bitmaps":[1,2,3,4]
            } 
            }`
        log.Println("POSTING:",msg)
        resp,err := http.Post("http://localhost:8089", "application/json", strings.NewReader(msg))
        defer resp.Body.Close()
        body, err := ioutil.ReadAll(resp.Body)
        log.Println(string(body),err)
        log.Println(">>>>DONE")

        
    }

    func TestServer(t *testing.T) {
        Convey("Run Server", t, func() {
            Stop := make(chan bool)
            Start := make(chan bool)
            go StartServer(":8089",Stop,Start)
            select{
            case <-Start:
                simple()
            case <-time.After(time.Duration(5) * time.Second):
            }
            Stop<- true
            So(0, ShouldEqual, 0)
        })
}
