package index

import (
    "testing"
    . "github.com/smartystreets/goconvey/convey"
    "net/http"
	"encoding/json"
"net/http/httptest"
//	"io/ioutil"
 //   "time"
    "log"
    "fmt"
    "strings"
    )
    
    func simple(id1,id2 int)string {
        return fmt.Sprintf(`{
            "Request": "UnionCount",
            "Fragment": "AAA-BBB-CCC",
            "Args": {  
                "Bitmaps":[%d,%d]
            } 
        }`,id1,id2)
    }
    func set_bit(id,pos int)string {
        return fmt.Sprintf(`{
            "Request": "SetBit",
            "Fragment": "AAA-BBB-CCC",
            "Args": {  
                "Bitmap_id":%d,
                "Bit_pos": %d
            } 
        }`,id,pos)
    }

    func sendRequest(msg string, dummy *FragmentContainer) (int,[]byte){

        r, err := http.NewRequest("POST", "http://api/foo", strings.NewReader(msg))
        if err != nil {
            log.Fatal(err)
        }

        w := httptest.NewRecorder()
 
        dummy.ServeHTTP(w , r ) 
        //return  w.Code, w.Body.String()
        return  w.Code, []byte(w.Body.String())


    }
	func getResult(key string ,s []byte )interface{}{
            var f interface{}
            err := json.Unmarshal(s, &f)
            if err != nil{

            log.Println(err)
            return nil
            }
            m := f.(map[string]interface{})
            x := m["results"]
            o := x.(map[string]interface{})
            return o[key]
        }

    func TestServer(t *testing.T) {
        dummy:=&FragmentContainer{make(map[string]*Fragment)}
        dummy.AddFragment("general", "25", 0, "AAA-BBB-CCC") 
         var ( 
            c int
            s []byte
        )

        Convey("Set Bit 1 1", t, func() {
            c,s=sendRequest(set_bit(1,1),dummy)
            So(c, ShouldEqual, 200)
            v:=getResult("value",s)
            So(v, ShouldEqual, 1)
        })

        Convey("Set Bit 2 2", t, func() {
            c,s=sendRequest(set_bit(2,2),dummy)
            So(c, ShouldEqual, 200)
            v:=getResult("value",s)
            So(v, ShouldEqual, 1)
        })

        Convey("Union", t, func() {
            c,s=sendRequest(simple(1,2),dummy)
            So(c, ShouldEqual, 200)
            v:=getResult("value",s)
            So(v, ShouldEqual, 2)
        })
}
